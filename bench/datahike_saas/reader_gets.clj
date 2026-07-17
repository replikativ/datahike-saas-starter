(ns datahike-saas.reader-gets
  "Drive exactly N derefs+queries from a DIRECT-S3 (non-streaming) reader against a
   tenant the writer already committed, printing the tenant's store id. Used by
   `bin/reader-gets`, which brackets a MinIO request trace around two runs (N and 2N
   derefs) and counts GetObject calls to derive the MARGINAL GETs/deref.

   The question it answers: Tier 3's whole premise is that a non-streaming `@conn`
   re-reads the branch head from the store, so a stateless reader auto-follows the
   writer with no sync. If that is true, each deref must cost at least one GET. This
   measures whether it actually does — the in-process timing cannot tell you, because
   a cached head read looks identical to no read at all."
  (:require [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.kernel.config :as config]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom]
            [datahike.http.writer])
  (:import [java.util UUID]))

(defn -main [& [n-str]]
  (let [n     (Long/parseLong (or n-str "20"))
        slug  (str "rg-" (subs (str (UUID/randomUUID)) 0 8))
        sid   (tenant/tenant-id->uuid slug)
        wpool (schema/create-pool)
        wc    (tenant/borrow wpool slug)]
    (dom/ensure-user! wc "alice" "Alice")
    (dotimes [i 5] (dom/create-issue! wc {:title (str "i" i) :reporter "alice"}))
    (tenant/close-all! wpool)                     ;; writer gone: reader is on its own

    ;; Fresh JVM-local reader pool, non-streaming writer backend, SAME shared store.
    (let [rbase (assoc (config/base-cfg) :writer {:backend :datahike-server
                                                  :url "http://localhost:8888"})
          rpool (schema/create-pool {:base-cfg rbase})
          rc    (tenant/borrow rpool slug)]
      (dotimes [_ 3] (dom/open-issues @rc))       ;; warm node cache; head still re-read
      (println "MARK-BEGIN")
      (dotimes [_ n] (dom/open-issues @rc))       ;; <-- the N derefs under measurement
      (println "MARK-END")
      (tenant/close-all! rpool))
    (println "STOREID" (str sid))
    (shutdown-agents)))
