(ns datahike-saas.tier4-gc-demo
  "Tier 4 frontend GC: a streaming read replica reclaims its local LMDB cache without
   disturbing the shared S3 backend, using konserve.gc/sweep! on the FRONTEND store.

   This runs a REAL Tier-4 replica, and it has to:

   - the **writer** is a kabel writer over the shared S3. It owns the bucket, and backend
     GC is its job.
   - the **replica** is a kabel client whose store is tiered `{lmdb, shared-s3}` with
     `:write-policy :frontend-only` — a cache it never writes back. Its LMDB starts EMPTY.

   Why the stream is not optional here. A tiered reader that merely `deref`s is FROZEN: the
   branch head is a MUTABLE key, `:read-policy :frontend-first` serves it out of the LMDB
   cache, and the reader therefore re-reads its own stale copy forever. That is exactly why
   Tier 3 forbids LMDB (doc/ladder.md) and why Tier 4 delivers the head over the kabel
   STREAM instead of re-reading it from the store. A deref-based tiered reader would sit on
   one snapshot, cache one generation, and report a tidy reclaim number that means nothing.

   Where a real replica's garbage comes from: konserve-sync pushes each commit's nodes into
   its LMDB as they are written, and read-through fills the rest. Both leave behind nodes
   that the NEXT commit supersedes, so a long-running replica accumulates objects no longer
   reachable from the head. That is what this reproduces, and what `sweep!` reclaims.

   Sweep is delete-only against a reachable whitelist, so it needs no full tree locally —
   only the reachable set (the `mark`), computed here against the SHARED BACKEND (the
   writer's authoritative head) or, in production, published by the writer, which already
   computes one during its own S3 GC.

   Run:
     docker compose --profile tier1 up -d
     SAAS_TIER=tier4 S3_BUCKET=tenants S3_ENDPOINT=http://localhost:9000 \\
       AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \\
       LMDB_PATH=$HOME/tmp/dh-tests/t4gc \\
       clj -M:bench:kabel:lmdb -m datahike-saas.tier4-gc-demo"
  (:require [datahike-saas.kernel.streaming :as streaming]
            [datahike.api :as d]
            [datahike.gc]                         ;; reachable-in-branch (private — see note)
            [konserve.gc :refer [sweep!]]
            [konserve.core :as k]
            [clojure.set :as set]
            [superv.async :refer [S <??]])
  (:import [java.util UUID Date]))

;; NOTE: datahike's reachability walk is private; a replica would normally take the
;; reachable set from the writer. We reach in here to keep the demo self-contained.
(def ^:private reachable-in-branch @#'datahike.gc/reachable-in-branch)

(defn- kcount [store] (count (<?? S (k/keys store {:sync? false}))))

(defn -main [& _]
  (let [url  (str "ws://localhost:8892")
        slug (str "t4gc-" (subs (str (UUID/randomUUID)) 0 8))
        w    (streaming/start-writer! {:ws-url url})]
    (try
      (let [wc (streaming/writer-conn w slug)]
        (d/transact wc [{:db/ident :k :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :v :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}])
        (doseq [b (partition-all 500 (range 2000))]
          (d/transact wc (mapv (fn [i] {:k i :v 0}) b)))

        (let [r  (streaming/start-reader! {:ws-url url})
              rc (streaming/reader-conn r slug)]
          (try
            (let [store    (:store @rc)
                  frontend (:frontend-store store)
                  backend  (:backend-store store)
                  config   (:config @rc)]
              (println (format "replica connected; frontend=%d  backend=%d  max-tx=%d"
                               (kcount frontend) (kcount backend) (:max-tx @rc)))

              ;; The writer churns. The replica FOLLOWS THE STREAM — its head advances, and
              ;; konserve-sync pushes each commit's nodes into its LMDB. The nodes it cached
              ;; a generation ago become unreachable.
              (println "writer churns 1000 updates; the replica follows the stream ...")
              (doseq [batch (partition-all 100 (range 1000))]
                (doseq [i batch] (d/transact wc [{:k (mod (* i 7) 2000) :v i}]))
                (Thread/sleep 300)                 ;; let the stream land
                (d/q '[:find ?e ?v :where [?e :v ?v]] @rc))

              (println (format "after churn;  frontend=%d  backend=%d  replica max-tx=%d (writer %d)"
                               (kcount frontend) (kcount backend) (:max-tx @rc) (:max-tx @wc)))
              (println (if (= (:max-tx @rc) (:max-tx @wc))
                         "  => the replica is CURRENT with the writer — it followed the stream"
                         "  => WARNING: the replica did NOT keep up; the numbers below are meaningless"))

              ;; MARK against the SHARED BACKEND — the writer's authoritative head. Walking the
              ;; tiered store would read the head through the LMDB cache, i.e. the replica's own
              ;; view rather than the truth.
              (let [branches  (<?? S (k/get backend :branches nil {:sync? false}))
                    reachable (-> (reduce (fn [acc b]
                                            (set/union acc (<?? S (reachable-in-branch
                                                                   backend b (Date. 0) config))))
                                          #{} branches)
                                  (conj :branches))
                    fk0 (kcount frontend) bk0 (kcount backend)]
                (println (format "before sweep: frontend=%d  backend=%d  (reachable=%d)"
                                 fk0 bk0 (count reachable)))

                ;; SWEEP THE FRONTEND. Delete-only, against the whitelist.
                (<?? S (sweep! frontend reachable (Date.)))

                (let [fk1  (kcount frontend) bk1 (kcount backend)
                      rows (count (d/q '[:find ?e ?v :where [?e :v ?v]] @rc))
                      last-churn? (pos? (count (d/q '[:find ?e :where [?e :v 999]] @rc)))]
                  (println (format "after sweep:  frontend=%d  backend=%d  (backend UNTOUCHED = the writer's job)"
                                   fk1 bk1))
                  (println (format "=> reclaimed %d/%d cached keys (%.0f%%); backend %s; replica reads %d rows and sees the LAST churn=%s"
                                   (- fk0 fk1) fk0
                                   (if (pos? fk0) (* 100.0 (/ (- fk0 fk1) fk0)) 0.0)
                                   (if (= bk0 bk1) "unchanged" (str "CHANGED " bk0 "->" bk1 " (BUG)"))
                                   rows last-churn?)))))
            (finally (streaming/stop-reader! r)))))
      (finally
        (streaming/stop-writer! w)
        (shutdown-agents)))))
