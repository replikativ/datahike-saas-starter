(ns datahike-saas.lifecycle
  "Tenant lifecycle: export, delete, restore.

   This is the payoff of db-per-tenant, and the reason the model is worth the trouble.
   Because a tenant IS a database — not rows tagged with a `tenant_id` — the operations
   that are genuinely hard in a shared-schema SaaS are ordinary here:

     offboard a customer   -> delete their database. No `DELETE FROM … WHERE tenant_id`
                              across 40 tables, no orphan rows, no fear that one table
                              was missed. `d/delete-database` removes their objects.
     GDPR erasure          -> the same operation, and it is complete BY CONSTRUCTION:
                              their data lives in no other tenant's database.
     export / takeout      -> read one database out as datoms.
     restore / clone       -> transact those datoms into a fresh database. Cloning a
                              tenant (staging copy, support repro) is the same code path.

   Note `d/delete-database` and `d/datoms` are **stable** Datahike API — unlike the
   write-amplification knobs this template also leans on, which are experimental.

   The pool is a cache of open connections, so mutating a tenant's database means
   evicting it first; `borrow` reopens on next use."
  (:require [datahike.api :as d]
            [datahike-saas.tenant :as tenant]
            [datahike-saas.schema :as schema]
            [replikativ.logging :as log]))

;; ── export ──────────────────────────────────────────────────────────────────

(defn export-tenant
  "Every datom in the tenant's current database, as `[e a v tx added]` vectors.

   Schema datoms are included, so the result is self-contained: `restore-tenant!`
   needs nothing but this. Reads the current value — for a point-in-time export,
   deref through `d/as-of` and pass that db instead (needs `:keep-history? true`).

   This materializes the whole tenant in memory, which is the right trade at the
   scale db-per-tenant is for (a tenant is MBs). Stream `d/datoms` if yours are big."
  [pool tenant-slug]
  (let [db (deref (tenant/borrow pool tenant-slug))]
    (into [] (map (juxt :e :a :v :tx :added)) (d/datoms db :eavt))))

(defn export-edn
  "`export-tenant` as an EDN string — the takeout format. Portable, diffable, and
   readable by anything; no Datahike required to inspect it."
  [pool tenant-slug]
  (pr-str {:tenant tenant-slug
           :store-id (str (tenant/tenant-id->uuid tenant-slug))
           :datoms (export-tenant pool tenant-slug)}))

;; ── delete ──────────────────────────────────────────────────────────────────

(defn delete-tenant!
  "Delete this tenant's database — the objects, not just the rows.

   Offboarding, and GDPR erasure, in one call. Because the tenant's data exists in
   no other database, the erasure is complete by construction: there is no other
   table to sweep and no `WHERE` clause to get wrong.

   Idempotent: deleting a tenant that does not exist is a no-op. Returns true if a
   database was removed.

   REFUSES to run on a Tier-4 replica pool. A replica's store is tiered {lmdb, shared-s3}
   with `:write-policy :frontend-only` — a cache over a bucket the WRITER owns and that the
   replica must never write. Deleting is the most destructive write there is: deleting a
   tenant from a replica would delete it out of the writer's bucket, for every node. Deletion
   is the writer's job, so we refuse rather than trust the layer below to be careful.
   konserve ≥ 0.9.359 also refuses at the store level (a `:frontend-only` tiered delete removes
   only the cache), so this is belt and braces — but a replica should not be asking in the first
   place, and the error here says *why* rather than silently doing half of what you asked."
  [pool tenant-slug]
  (let [cfg   (tenant/tenant-cfg pool tenant-slug)
        store (get-in pool [:base-cfg :store])]
    (when (and (= :tiered (:backend store))
               (= :frontend-only (:write-policy store)))
      (throw (ex-info (str "Refusing to delete a tenant from a READ REPLICA. This node's store "
                           "is a :frontend-only cache over the writer's bucket; deleting here "
                           "would delete the tenant out of the shared, writer-owned store. "
                           "Send the delete to the writer.")
                      {:type :saas/delete-on-replica :tenant tenant-slug
                       :store-backend (:backend store) :write-policy (:write-policy store)})))
    (tenant/evict! pool tenant-slug)                 ;; drop the cached connection first
    (if (d/database-exists? cfg)
      (do (d/delete-database cfg)
          (log/info :tenant/deleted {:tenant tenant-slug
                                     :store-id (str (tenant/tenant-id->uuid tenant-slug))})
          true)
      false)))

;; ── restore / clone ─────────────────────────────────────────────────────────

(defn restore-tenant!
  "Recreate `target-slug` from an `export-tenant` datom vector.

   Restore (same slug) and clone (different slug) are the same operation — which is
   how you get a staging copy of a customer, or reproduce a support ticket against
   their real data, without touching production.

   Entity ids are NOT preserved — the target assigns its own — so every `:db.type/ref` has
   to be rewritten, or the restore silently drops the graph. A ref's stored value is an
   entity id in the SOURCE database; replaying it verbatim points at nothing (or, worse, at
   whatever unrelated entity happens to hold that id in the target). Two kinds of ref, two
   rewrites:

     ref -> another exported entity  (:issue/reporter, :issue/assignee, :issue/labels,
                                      :issue/comments, :comment/author)
        becomes a TEMPID derived from the source id, so the whole graph is transacted as one
        connected unit and Datahike wires it up with fresh ids.

     ref -> a schema/enum entity     (:issue/state, :issue/priority)
        becomes its `:db/ident` keyword (`:issue.priority/urgent`), which resolves by NAME in
        the target. Leaning on the id happening to coincide would work only by luck.

   Refuses to overwrite an existing database."
  [pool target-slug datoms]
  (let [cfg (tenant/tenant-cfg pool target-slug)]
    (when (d/database-exists? cfg)
      (throw (ex-info "Refusing to restore over an existing tenant — delete it first."
                      {:tenant target-slug})))
    (d/create-database cfg)
    (let [conn (tenant/borrow pool target-slug)]
      (schema/ensure-schema! conn)                 ;; schema first: attribute types must be known
      (let [db        @conn
            alive     (remove (fn [[_ _ _ _ added]] (false? added)) datoms)
            by-e      (group-by first alive)
            ;; source eid -> :db/ident, read out of the export itself: schema and enum
            ;; entities carry their own :db/ident datom.
            ident-of  (into {} (for [[e ds] by-e, [_ a v] ds :when (= a :db/ident)] [e v]))
            ;; Domain entities = those with no :db/* attribute. The schema entities in the
            ;; export were just installed by ensure-schema!; replaying them would fight it.
            domain?   (fn [ds] (not-any? (fn [[_ a _]] (= "db" (namespace a))) ds))
            domain    (into {} (filter (comp domain? val)) by-e)
            ref?      (fn [a] (= :db.type/ref (:db/valueType (d/entity db a))))
            tempid    (fn [e] (- (long e)))          ;; source ids are positive; negate for a tempid
            resolve-v (fn [a v]
                        (if (ref? a)
                          (cond
                            (contains? domain v) (tempid v)      ;; -> another restored entity
                            (ident-of v)         (ident-of v)    ;; -> :issue.priority/urgent
                            :else                v)
                          v))
            tx-data   (for [[e ds] domain
                            :let [m (reduce (fn [acc [_ a v]]
                                              (let [v' (resolve-v a v)]
                                                (if (contains? acc a)
                                                  (update acc a #(if (vector? %) (conj % v') [% v']))
                                                  (assoc acc a v'))))
                                            {:db/id (tempid e)} ds)]
                            :when (> (count m) 1)]   ;; more than just :db/id
                        m)]
        (when (seq tx-data)
          (d/transact conn (vec tx-data)))
      (log/info :tenant/restored {:tenant target-slug :entities (count tx-data)}))
      conn)))
