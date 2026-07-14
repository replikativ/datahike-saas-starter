(ns datahike-saas.tier4-sync-test
  "Tier-4 streaming guarantees, against a real object store (MinIO on localhost:9000).

   These pin the three properties Tier 4 actually rests on. Each was broken at some point,
   and the first two failed SILENTLY — the reader kept answering every query correctly,
   because a missing node just falls through to the shared S3. Only a direct measurement of
   the local cache, and of how the head arrives, catches a regression.

     1. ACTIVE WARM — konserve-sync pushes a commit's nodes into the reader's LMDB.
        Regression here degrades Tier 4 into Tier 3 with extra steps: every read
        round-trips to S3 and the whole point (a bounded local cache) is gone, while
        every test still passes.

     2. HEAD DELIVERED BY THE HANDSHAKE — not read out of a local cache. The head is
        the one MUTABLE cell, and the sync used to dedup it away on a wall-clock
        timestamp comparison across two machines, so a tiered reader never received
        one. It only worked because the connector read a cached head instead.

     3. READER RESTART onto a warm cache. Ordinary in a deployment, and it was fatal twice
        over: it used to DEADLOCK (an up-to-date subscriber was deduped away by a wall-clock
        timestamp comparison, so the head was never sent), and then it used to SIGSEGV
        (konserve-lmdb closed the LMDB env without draining, munmapping while cache warms
        were still inside mdb_txn_commit). Both fixed upstream; testing it in-process is how
        we find out if either comes back.

   Needs the streaming + LMDB deps:
     docker compose --profile tier1 up -d
     SAAS_TIER=tier4 LMDB_PATH=~/tmp/t4-ci clj -X:kabel:lmdb:integration-tier4

   SAAS_TIER=tier4 is REQUIRED: without it `config/base-cfg` returns tier1's plain :s3
   store and every test errors with 'no tiered store found'."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike-saas.streaming :as st]
            [datahike.api :as d]
            [konserve.core :as k]
            [clojure.core.async :refer [<!!]]
            [konserve-s3.core :as s3])
  (:import [java.util UUID]))

(def ^:private ws-url "ws://localhost:8899")

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

(defn- tiered-of
  "The reader's TieredStore, wherever it sits in the store chain."
  [conn]
  (loop [s (:store @conn), depth 0]
    (cond
      (> depth 8)          (throw (ex-info "no tiered store found" {}))
      (:frontend-store s)  s
      (:store s)           (recur (:store s) (inc depth))
      :else                (throw (ex-info "no tiered store found" {})))))

(defn- lmdb-key-count [conn]
  (count (<!! (k/keys (:frontend-store (tiered-of conn))))))

(defn- s3-key-count [conn]
  (count (<!! (k/keys (:backend-store (tiered-of conn))))))

(deftest tier4-streaming-warms-the-local-cache
  (testing "a live commit's nodes are PUSHED into the reader's LMDB, not merely readable"
    (let [slug   (str "t4sync" (subs (str (UUID/randomUUID)) 0 8))
          writer (st/start-writer! {:ws-url ws-url})]
      (try
        (let [wc (st/writer-conn writer slug)]
          (d/transact wc {:tx-data [{:db/ident       :note/title
                                     :db/valueType   :db.type/string
                                     :db/cardinality :db.cardinality/one}]})
          ;; Enough to spill the index into separately-stored child nodes. A small
          ;; commit stays inline/buffered (fused roots + diff-buf) and writes NO new
          ;; objects at all — there would be nothing to stream, and the test would
          ;; pass vacuously.
          (d/transact wc {:tx-data (vec (for [i (range 4000)]
                                          {:note/title (str "seed-" i)}))})

          (let [reader (st/start-reader! {:ws-url ws-url})]
            (try
              (let [rc          (st/reader-conn reader slug)
                    _           (Thread/sleep 2000)
                    lmdb-before (lmdb-key-count rc)
                    s3-before   (s3-key-count rc)]

                (is (= 4000 (count (d/q '[:find ?e :where [?e :note/title]] @rc)))
                    "reader sees the seeded state")

                ;; live commit, large enough to flush new nodes to S3
                (d/transact wc {:tx-data (vec (for [i (range 4000)]
                                                {:note/title (str "live-" i)}))})
                (Thread/sleep 3000)

                (let [s3-after   (s3-key-count rc)
                      lmdb-after (lmdb-key-count rc)]
                  ;; guard the test itself: if the writer flushed nothing, there is
                  ;; nothing to stream and a green result would mean nothing.
                  (is (> s3-after s3-before)
                      "precondition: the commit actually wrote new nodes to S3")

                  (is (> lmdb-after lmdb-before)
                      "the commit's nodes were PUSHED into the reader's LMDB (active warm)")

                  (is (= 8000 (count (d/q '[:find ?e :where [?e :note/title]] @rc)))
                      "and the reader follows the live commit")))
              (finally (st/stop-reader! reader)))))
        (finally (st/stop-writer! writer))))))

(deftest tier4-reconnect-onto-a-warm-cache
  (testing "a reader restarting onto a populated LMDB completes and reuses it"
    ;; This closes one reader and starts another on the SAME LMDB path, in the SAME JVM —
    ;; two things that used to be fatal, for two different reasons:
    ;;
    ;;   * it used to DEADLOCK. An up-to-date subscriber was deduped away by a wall-clock
    ;;     timestamp comparison, so the branch head was never sent and the connect gate
    ;;     waited forever. (konserve-sync :always-send-mutable?)
    ;;   * then it used to SIGSEGV. konserve-lmdb's release-store called mdb_env_close with
    ;;     no drain, munmapping the data file while konserve's fire-and-forget cache warms
    ;;     were still inside mdb_txn_commit. (konserve-lmdb 0.1.16 drains before closing.)
    ;;
    ;; Both are fixed, so this is testable in-process again — and it is worth testing,
    ;; because a reader restart is the most ordinary thing a deployment does.
    (let [slug   (str "t4re" (subs (str (UUID/randomUUID)) 0 8))
          writer (st/start-writer! {:ws-url ws-url})]
      (try
        (let [wc (st/writer-conn writer slug)]
          (d/transact wc {:tx-data [{:db/ident       :note/title
                                     :db/valueType   :db.type/string
                                     :db/cardinality :db.cardinality/one}]})
          (d/transact wc {:tx-data (vec (for [i (range 4000)]
                                          {:note/title (str "n-" i)}))})

          ;; first reader warms its cache, then shuts down
          (let [r1     (st/start-reader! {:ws-url ws-url})
                warmed (try
                         (let [c (st/reader-conn r1 slug)]
                           (Thread/sleep 2000)
                           (is (= 4000 (count (d/q '[:find ?e :where [?e :note/title]] @c)))
                               "first reader sees the state")
                           (lmdb-key-count c))
                         (finally (st/stop-reader! r1)))]

            ;; second reader, same LMDB path, same JVM
            (let [r2 (st/start-reader! {:ws-url ws-url})]
              (try
                (let [c (st/reader-conn r2 slug)]
                  (Thread/sleep 2000)
                  (is (= 4000 (count (d/q '[:find ?e :where [?e :note/title]] @c)))
                      "restart onto a warm cache yields a queryable db (used to deadlock)")
                  (is (>= (lmdb-key-count c) warmed)
                      "and the existing cache is reused, not re-fetched"))
                (finally (st/stop-reader! r2))))))
        (finally (st/stop-writer! writer))))))
