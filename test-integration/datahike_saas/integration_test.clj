(ns datahike-saas.integration-test
  "End-to-end against a real object store (MinIO on localhost:9000). Run by the
   CircleCI `integration` job (MinIO service container) and locally via
   `clj -M:integration` after `docker compose --profile tier1 up -d`.

   Kept out of the default `test/` path so `clj -M:test` (in-memory) needs no
   object store."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike-saas.tenant :as tenant]
            [datahike-saas.domain :as dom]
            [datahike-saas.lifecycle :as lc]
            [datahike.api :as d]
            [konserve-s3.core :as s3]))

(defn- has-attr? [db ident]
  (some? (d/entity db ident)))

(defn- ensure-bucket! []
  (let [spec   {:region "us-east-1" :access-key "minioadmin" :secret "minioadmin"
                :path-style-access? true
                :endpoint-override {:protocol :http
                                    :hostname (or (System/getenv "MINIO_HOST") "localhost")
                                    :port     (Integer/parseInt (or (System/getenv "MINIO_PORT") "9000"))}}
        client (s3/s3-client spec)]
    (when-not (s3/bucket-exists? client "tenants")
      (s3/create-bucket client "tenants"))))

(use-fixtures :once (fn [f] (ensure-bucket!) (f)))

(deftest tier1-minio-roundtrip
  (testing "a tenant's issues round-trip through MinIO-backed Datahike"
    (let [pool (tenant/create-pool)                     ; tier1 = :s3 @ localhost:9000
          slug (str "ci-" (random-uuid))
          c    (tenant/borrow pool slug)]
      (try
        (let [{:keys [id number]} (dom/create-issue!
                                   c {:title "CI smoke test" :reporter "alice"
                                      :assignee "bob" :priority :issue.priority/high :labels ["bug"]})]
          (is (= 1 number))
          (dom/comment! c id "bob" "seen it")
          (is (= [1] (map :issue/number (dom/open-issues @c))))
          (is (= "bob" (get-in (dom/issue @c id) [:issue/assignee :user/handle])))
          (is (= 1 (count (:issue/comments (dom/issue @c id)))))
          (is (= {:issue.state/open 1} (:by-state (dom/stats @c))))
          (testing "reconnect sees the durable state"
            (tenant/close-all! pool)
            (let [pool2 (tenant/create-pool)
                  c2    (tenant/borrow pool2 slug)]
              (is (= [1] (map :issue/number (dom/open-issues @c2))))
              (tenant/close-all! pool2))))
        (finally (tenant/close-all! pool))))))

;; ── tenant lifecycle: export / clone / delete ────────────────────────────────
;;
;; This is the argument for db-per-tenant, so it gets asserted, not asserted-about.
;;
;; The trap it guards: entity ids are NOT stable across databases. A `:db.type/ref`
;; datom's value is an entity id in the SOURCE db, and this schema is ref-heavy
;; (:issue/reporter, :issue/assignee, :issue/labels, :issue/comments, :comment/author,
;; plus the :issue/state and :issue/priority enums). Replay those values verbatim into a
;; fresh database and the refs land on nothing — the clone still has its issues, still
;; reports the right counts, and has quietly lost its whole graph. Scalars survive, so a
;; test that only checks titles and stats passes. Hence: assert the REFS.

(deftest tenant-export-clone-delete
  (let [pool (tenant/create-pool)
        src  (str "lc-src-" (random-uuid))
        dst  (str "lc-dst-" (random-uuid))]
    (try
      (let [c (tenant/borrow pool src)
            {:keys [id]} (dom/create-issue!
                          c {:title "Login rejects valid tokens" :reporter "alice"
                             :assignee "bob" :priority :issue.priority/urgent
                             :labels ["bug" "auth"]})]
        (dom/comment! c id "bob" "reproduced on staging")

        (testing "export is self-contained"
          (let [datoms (lc/export-tenant pool src)]
            (is (seq datoms))
            (is (string? (lc/export-edn pool src)))

            (testing "clone preserves the GRAPH, not just the scalars"
              (lc/restore-tenant! pool dst datoms)
              ;; :issue/id is a uuid (unique identity, a scalar) so it survives verbatim —
              ;; pull the SAME issue out of the clone and compare the whole graph.
              (let [orig  (dom/issue @(tenant/borrow pool src) id)
                    clone (dom/issue @(tenant/borrow pool dst) id)]
                (is (= (:issue/title orig) (:issue/title clone)))
                (is (= (:issue/number orig) (:issue/number clone)))
                ;; refs -> other restored entities
                (is (= "alice" (get-in clone [:issue/reporter :user/handle]))
                    "reporter ref must survive the clone")
                (is (= "bob" (get-in clone [:issue/assignee :user/handle]))
                    "assignee ref must survive the clone")
                (is (= #{"bug" "auth"} (set (map :label/name (:issue/labels clone))))
                    "cardinality-many label refs must survive the clone")
                (is (= 1 (count (:issue/comments clone)))
                    "component comment must survive the clone")
                (is (= "reproduced on staging" (:comment/body (first (:issue/comments clone)))))
                (is (= "bob" (get-in (first (:issue/comments clone)) [:comment/author :user/handle]))
                    "ref FROM a component entity must survive the clone")
                ;; refs -> enum entities, resolved by :db/ident, not by a coincident id
                (is (= :issue.priority/urgent (get-in clone [:issue/priority :db/ident]))
                    "enum ref must survive the clone")
                (is (= :issue.state/open (get-in clone [:issue/state :db/ident])))))))

        (testing "restore refuses to overwrite an existing tenant"
          (is (thrown? Exception (lc/restore-tenant! pool dst (lc/export-tenant pool src)))))

        (testing "delete removes the database, and is idempotent"
          (is (true? (lc/delete-tenant! pool src)))
          (is (false? (d/database-exists? (tenant/tenant-cfg pool src)))
              "delete-tenant! must actually delete the database, not just the rows")
          (is (false? (lc/delete-tenant! pool src)) "deleting a missing tenant is a no-op"))

        (testing "the clone is independent — deleting the source did not touch it"
          (is (= ["Login rejects valid tokens"]
                 (map :issue/title (dom/open-issues @(tenant/borrow pool dst)))))))
      (finally
        (try (lc/delete-tenant! pool src) (catch Exception _))
        (try (lc/delete-tenant! pool dst) (catch Exception _))
        (tenant/close-all! pool)))))

;; ── lazy migrations ─────────────────────────────────────────────────────────
;;
;; The objection that kills db-per-tenant elsewhere is "do I now run 10,000 migrations?"
;; The answer here is that you don't run them at all: each tenant applies pending norms
;; the first time it is opened after the deploy (`tenant/migrate!`). No window, no
;; orchestrator, no big-bang — and a partial rollout is not a broken state, because every
;; database knows which norms it has.

(deftest tenant-migrates-lazily-on-first-touch
  (let [pool (tenant/create-pool)
        slug (str "mig-" (random-uuid))]
    (try
      (testing "a tenant opened after the deploy has the migration applied"
        (let [c (tenant/borrow pool slug)]
          (is (has-attr? @c :issue/severity)
              ":issue/severity comes from resources/migrations/001 — applied on first touch")))

      (testing "re-opening does not re-apply it (norms are stamped, not replayed)"
        (tenant/evict! pool slug)
        (let [c (tenant/borrow pool slug)]
          (is (has-attr? @c :issue/severity))
          ;; The attribute is usable immediately. Note we assert against the DATABASE, not
          ;; via `dom/issue` — its pull pattern is a fixed list that doesn't mention
          ;; :issue/severity yet. That is expand/contract in miniature: the schema migrates
          ;; first, the app catches up after, and both halves must tolerate the gap.
          (let [{:keys [id]} (dom/create-issue! c {:title "Disk filling up" :reporter "alice"})]
            (d/transact c [{:db/id [:issue/id id] :issue/severity 4}])
            (is (= #{[4]} (d/q '[:find ?sev :in $ ?id :where
                                 [?e :issue/id ?id] [?e :issue/severity ?sev]]
                               @c id))
                "the migrated attribute is usable straight away")
            (is (= 1 (count (dom/open-issues @c)))
                "and the un-migrated app code keeps working — old issues simply lack it"))))

      (finally
        (try (lc/delete-tenant! pool slug) (catch Exception _))
        (tenant/close-all! pool)))))
