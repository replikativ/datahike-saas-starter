(ns datahike-saas.tenant
  "Lazy per-tenant connection registry.

   Each tenant is its own Datahike database (db-per-tenant): isolation is total, a tenant's
   working set is its own, and a single-writer model applies per tenant rather than globally.
   A connection is opened on first `borrow` and kept hot until it is evicted.

   BOUNDED, because a hot connection is not free: it holds that tenant's in-memory index, and
   that scales with the tenant's DATA, not with the tenant count. Measured (`workload scale`):

       5 issues/tenant   ->  0.09 MB/tenant   (a toy tenant)
     100 issues/tenant   ->  1.30 MB/tenant   (14x, and roughly linear in the data)

   An unbounded registry therefore grows with every tenant EVER TOUCHED and never gives the
   memory back — 10k tenants at 1.3 MB is ~13 GB, and a fatter tenant profile is far worse.
   So the pool keeps at most `:max-hot` connections (env `SAAS_MAX_HOT`, default 512) and
   evicts the least-recently-used beyond that. Heap then tracks the CONCURRENTLY ACTIVE set —
   which is what real traffic looks like: a hot head and a long idle tail — instead of the
   whole fleet. `:max-hot nil` restores the unbounded behaviour (the density benchmark wants
   it, to measure MB/tenant with everything held open).

   Eviction is safe, and that is the whole difficulty. A connection must NEVER be closed while
   a request is reading it — that would release the store out from under an in-flight query.
   So each entry carries an in-flight count: `pin!`/`unpin!` bracket a request (see
   `core/wrap-tenant-pin`), and eviction skips any tenant with in-flight work. A pinned tenant
   is never evicted, no matter how old; if EVERY tenant is pinned we exceed :max-hot rather
   than break a live request. Time-based eviction with a grace window would be simpler and
   would occasionally close a slow query's connection underneath it — this doesn't.

   Cost of an eviction: the next request reopens the tenant and lazily faults its index nodes
   back in from the store. That is a warm reconnect, not the cold first-connect (which pays
   `create-database` + schema install). See doc/benchmarks.md §3b."
  (:require [datahike.api :as d]
            [datahike.norm.norm :as norm]
            [datahike-saas.config :as config]
            [datahike-saas.schema :as schema]
            [clojure.java.io :as io]
            [konserve-s3.core]                    ;; registers :s3 backend
            [replikativ.logging :as log])
  (:import [java.security MessageDigest]
           [java.util UUID]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]
           [java.util.function Function]))

;; ── tenant slug → stable store id ───────────────────────────────────────────

(def ^:private namespace-uuid
  #uuid "00000000-0000-0000-0000-000000000001")

(defn tenant-id->uuid
  "Deterministic v5-style UUID from a tenant slug, so a slug always maps to the
   same store id across process restarts."
  [tenant-slug]
  (let [bytes (.getBytes (str namespace-uuid tenant-slug) "UTF-8")
        md5   (.digest (MessageDigest/getInstance "MD5") bytes)]
    (aset-byte md5 6 (unchecked-byte (bit-or 0x50 (bit-and (aget md5 6) 0x0F))))
    (aset-byte md5 8 (unchecked-byte (bit-or 0x80 (bit-and (aget md5 8) 0x3F))))
    (let [msb (reduce (fn [a i] (bit-or (bit-shift-left a 8) (bit-and (aget md5 i) 0xFF))) 0 (range 0 8))
          lsb (reduce (fn [a i] (bit-or (bit-shift-left a 8) (bit-and (aget md5 i) 0xFF))) 0 (range 8 16))]
      (UUID. msb lsb))))

(defn tenant-cfg
  "Per-tenant Datahike config: the shared tier base-cfg with this tenant's store id.

   How a tenant is separated depends on the backend:
   - `:s3` — by store id. konserve-s3 prefixes every key with it, so all tenants share one
     bucket and one set of credentials. This is the whole economic argument.
   - `:file` — by DIRECTORY. A konserve file store *is* a folder, and its keys are files
     inside it with no store-id prefix, so tenants sharing a path would share a key space.
     Each tenant gets `<path>/<store-id>/`.
   - `:tiered` (Tier 4) — the id goes on both layers, and the LMDB frontend gets its own
     directory for the same reason as `:file` (the S3 backend is keyed by store id)."
  [{:keys [base-cfg]} tenant-slug]
  (let [sid   (tenant-id->uuid tenant-slug)
        store (:store base-cfg)]
    (assoc base-cfg :store
           (case (:backend store)
             :tiered (-> store
                         (assoc :id sid)
                         (assoc-in [:frontend-config :id] sid)
                         (assoc-in [:backend-config :id] sid)
                         (update-in [:frontend-config :path] str "/" sid))
             :file   (-> store
                         (assoc :id sid)
                         (update :path str "/" sid))
             (assoc store :id sid)))))

;; ── open / close ────────────────────────────────────────────────────────────

(defn- migrate!
  "Apply any pending migrations to this tenant, LAZILY — on the first touch after a deploy.

   `datahike.norm` transacts every EDN norm under `resources/migrations/` that this database
   has not seen, in filename order, and stamps each with `:tx/norm` so it is never applied
   twice. Idempotent, per-database, and resumable by construction.

   This is the answer to the objection that kills db-per-tenant elsewhere: 'do I now run
   10,000 migrations?' You don't run them at all — each tenant migrates itself when it is
   next used, so the cost amortizes into normal traffic. No maintenance window, no
   orchestrator, no big-bang. An idle tenant migrates when it wakes; a tenant that never
   wakes costs nothing. A partial rollout is not a broken state — every database knows which
   norms it has, and re-running converges.

   Measured: an additive norm is ~31 ms/tenant (one commit, 1 PUT) — adding an attribute is a
   small transaction, not an ALTER TABLE, because datoms are sparse. See doc/migrations.md."
  [conn tenant-slug]
  (when-let [migrations (io/resource "migrations")]
    (let [t0 (System/nanoTime)]
      (norm/ensure-norms! conn migrations)
      (log/debug :tenant/migrated {:tenant tenant-slug
                                   :cost-ms (/ (- (System/nanoTime) t0) 1e6)}))))

(defn- open-tenant!
  "Create (if missing), connect, ensure schema, and apply pending migrations for a tenant db.
   Returns {:conn :cfg :open-cost-ns}."
  [pool tenant-slug]
  (let [cfg (tenant-cfg pool tenant-slug)
        t0  (System/nanoTime)]
    (when-not (d/database-exists? cfg)
      (d/create-database cfg))
    (let [conn (d/connect cfg)]
      (schema/ensure-schema! conn)
      (migrate! conn tenant-slug)                  ;; lazy, idempotent, per-tenant
      {:conn conn :cfg cfg :open-cost-ns (- (System/nanoTime) t0)})))

;; ── registry ────────────────────────────────────────────────────────────────

(defn- env-max-hot []
  (when-let [v (System/getenv "SAAS_MAX_HOT")]
    (let [n (Long/parseLong v)]
      (when (pos? n) n))))                            ;; SAAS_MAX_HOT=0 => unbounded

(defn create-pool
  "A tenant pool. `:base-cfg` defaults to the active tier's config. For a :tiered store
   (Tier 4) the :lmdb backend is loaded on demand (needs the :lmdb alias).

   `:max-hot` bounds how many connections stay open (default: env SAAS_MAX_HOT, else 512).
   `nil` (or SAAS_MAX_HOT=0) means unbounded — the old behaviour, and what the density
   benchmark wants."
  ([] (create-pool {}))
  ([{:keys [base-cfg max-hot] :or {max-hot :default}}]
   (let [cfg (or base-cfg (config/base-cfg))]
     (when (= :tiered (get-in cfg [:store :backend]))
       (require 'datahike-lmdb.core))                 ;; registers the :lmdb backend
     {:base-cfg cfg
      :tenants  (ConcurrentHashMap.)
      ;; Pins are keyed by SLUG, not by entry: a tenant is pinned for the life of the request,
      ;; which BEGINS BEFORE its connection exists (the first request for a tenant opens it).
      ;; Pinning the entry instead would leave that first connection unprotected between
      ;; `borrow` creating it and the handler reading it.
      :pins     (ConcurrentHashMap.)
      :max-hot  (if (= max-hot :default) (or (env-max-hot) 512) max-hot)
      :clock    (AtomicLong. 0)})))                   ;; monotonic tick — LRU ordering, no wall clock

(defn- touch! [pool entry]
  (.set ^AtomicLong (:used entry) (.incrementAndGet ^AtomicLong (:clock pool))))

(defn pin!
  "Mark a tenant as IN USE. A pinned tenant is never evicted, however old. Must be paired
   with `unpin!` in a finally — `core/wrap-tenant-pin` does this for every HTTP request."
  [pool tenant-slug]
  (let [^ConcurrentHashMap pins (:pins pool)]
    (.incrementAndGet ^AtomicLong
                      (.computeIfAbsent pins tenant-slug
                                        (reify Function (apply [_ _] (AtomicLong. 0)))))))

(defn unpin! [pool tenant-slug]
  (let [^ConcurrentHashMap pins (:pins pool)]
    (when-let [^AtomicLong n (.get pins tenant-slug)]
      (when (<= (.decrementAndGet n) 0)
        (.remove pins tenant-slug n)))))              ;; CAS-remove; a racing pin! re-adds

(defn- pinned? [pool tenant-slug]
  (when-let [^AtomicLong n (.get ^ConcurrentHashMap (:pins pool) tenant-slug)]
    (pos? (.get n))))

(defn- evict-lru!
  "Close least-recently-used connections until the pool is within :max-hot.

   Never evicts a PINNED tenant (a request is reading it), and never evicts `keep` — the
   tenant we are in the middle of borrowing. Without that second exclusion a full pool whose
   other entries are all pinned would evict the entry `borrow` just created and hand the
   caller a released connection. So the bound yields to liveness: if there is nothing safe to
   close, we exceed :max-hot rather than break a live request."
  [pool keep]
  (when-let [max-hot (:max-hot pool)]
    (let [^ConcurrentHashMap m (:tenants pool)]
      (loop []
        (when (> (.size m) max-hot)
          (let [victim (->> (into [] m)
                            (remove (fn [[slug _]] (or (= slug keep) (pinned? pool slug))))
                            (sort-by (fn [[_ e]] (.get ^AtomicLong (:used e))))
                            first)]
            (when victim
              (let [[slug e] victim]
                ;; .remove(k,v) is a CAS: only evict if this is still the entry we chose.
                (when (.remove m slug e)
                  (try (d/release (:conn e))
                       (catch Exception ex
                         (log/warn :tenant/evict-failed {:tenant slug :error (.getMessage ex)})))
                  (log/debug :tenant/evicted {:tenant slug :hot (.size m) :max-hot max-hot}))
                (recur)))))))))

(defn borrow
  "Return the Datahike connection for a tenant, opening it on first use and marking it as
   most-recently-used. May evict other (unpinned) tenants to stay within :max-hot."
  [pool tenant-slug]
  (let [^ConcurrentHashMap m (:tenants pool)
        entry (.computeIfAbsent
               m tenant-slug
               (reify Function
                 (apply [_ slug]
                   (let [e (open-tenant! pool slug)]
                     (log/info :tenant/opened {:tenant slug :cost-ms (/ (:open-cost-ns e) 1e6)})
                     (assoc e :used (AtomicLong. 0))))))]
    (touch! pool entry)
    (evict-lru! pool tenant-slug)
    (:conn entry)))

(defn hot-count [pool] (.size ^ConcurrentHashMap (:tenants pool)))

(defn close-all!
  "Release every open tenant connection."
  [pool]
  (let [^ConcurrentHashMap m (:tenants pool)]
    (doseq [[slug {:keys [conn]}] (into {} m)]
      (try (d/release conn)
           (catch Exception e (log/warn :tenant/close-failed {:tenant slug :error (.getMessage e)}))))
    (.clear m)))

(defn evict!
  "Release this tenant's connection and drop it from the registry. The database on
   the store is untouched — the next `borrow` reopens it."
  [pool tenant-slug]
  (let [^ConcurrentHashMap m (:tenants pool)]
    (when-let [{:keys [conn]} (.remove m tenant-slug)]
      (try (d/release conn)
           (catch Exception e
             (log/warn :tenant/close-failed {:tenant tenant-slug :error (.getMessage e)})))
      true)))
