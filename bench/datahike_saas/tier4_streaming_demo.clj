(ns datahike-saas.tier4-streaming-demo
  "Tier 4 (streaming + lmdb+s3): one writer streams commits to read replicas whose
   store is TIERED {lmdb, shared-s3}. Runs both peers in one JVM (separate processes
   in production) and shows:
     1. a reader syncs a tenant from the writer and serves reads from its LOCAL LMDB;
     2. a commit on the writer propagates to the reader (it follows the stream — so
        the head is fresh with no per-deref S3 read and no stale-head caching);
     3. reader reads never touch S3 — they're local-LMDB fast, and durable across
        restarts (the LMDB persists).

   Run (S3 backend = local MinIO via env):
     docker compose --profile tier1 up -d
     SAAS_TIER=tier4 S3_BUCKET=tenants S3_ENDPOINT=http://localhost:9000 \\
       AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \\
       clj -M:bench:kabel:lmdb -m datahike-saas.tier4-streaming-demo"
  (:require [datahike-saas.streaming :as streaming]
            [datahike-saas.domain :as dom]
            [datahike-saas.harness :as h]
            [datahike.api :as d])
  (:import [java.util UUID]))

(defn -main [& _]
  (let [url  (str "ws://localhost:8891")
        slug (str "t3-" (subs (str (UUID/randomUUID)) 0 8))
        w    (streaming/start-writer! {:ws-url url})]
    (try
      ;; writer: create + seed the tenant, register it for remote access
      (let [wc (streaming/writer-conn w slug)]
        (dom/ensure-user! wc "alice" "Alice")
        (dotimes [i 5] (dom/create-issue! wc {:title (str "Seeded issue " i) :reporter "alice"}))
        (println "writer seeded" slug "with 5 issues")

        ;; reader: connect, open a following conn (syncs from writer)
        (let [r  (streaming/start-reader! {:ws-url url})
              rc (streaming/reader-conn r slug)]
          (Thread/sleep 800)                         ;; let the initial sync settle
          (println "reader sees on connect:" (map :issue/number (dom/open-issues @rc)))

          ;; writer commits a NEW issue directly; reader should follow it
          (dom/create-issue! wc {:title "Live update from writer" :reporter "alice"
                                 :priority :issue.priority/urgent})
          (Thread/sleep 1200)
          (println "reader after live writer commit:" (map :issue/number (dom/open-issues @rc)))
          (println "reader sees the urgent issue:"
                   (map :issue/title (dom/by-priority @rc :issue.priority/urgent)))

          ;; measure reader read latency — served from the in-memory index, no S3
          (let [hh (h/hist)]
            (dotimes [_ 500]
              (let [[_ ns] (h/time-ns #(dom/open-issues @rc))] (h/record! hh ns)))
            (println "reader read latency (local, no S3):" (h/summary hh)))

          (streaming/stop-reader! r)))
      (finally
        (streaming/stop-writer! w)
        (shutdown-agents)))))
