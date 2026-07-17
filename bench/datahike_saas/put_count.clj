(ns datahike-saas.put-count
  "Drive exactly N create-issue commits into a fresh tenant under a chosen store
   config variant, printing the tenant's store id. Used by `bin/put-count`, which
   brackets a MinIO request trace around two runs (N and 2N commits) and counts
   PutObject calls to derive the MARGINAL PUTs/commit.

   Why a trace and not the in-process key-set delta: a commit overwrites the
   branch head at a FIXED konserve key — a real PUT that adds no new key — so the
   key-set delta undercounts PUTs. PUTs/commit is the write metric that maps to
   object-store cost ($/PUT) and latency; net key-set growth (workload.clj
   `compare`) is the separate storage/GC metric."
  (:require [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.kernel.config :as config]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom])
  (:import [java.util UUID]))

(def variants
  {:economical        {:fuse-index-roots? true  :commit-graph? false :keep-history? false :index-config {:diff-buf-size 256}}
   :no-fusion         {:fuse-index-roots? false :commit-graph? false :keep-history? false :index-config {:diff-buf-size 256}}
   :no-diff-buf       {:fuse-index-roots? true  :commit-graph? false :keep-history? false :index-config {:diff-buf-size 0}}
   :with-commit-graph {:fuse-index-roots? true  :commit-graph? true  :keep-history? false :index-config {:diff-buf-size 256}}})

(defn -main [& [n-str variant-str]]
  (let [n       (Long/parseLong (or n-str "20"))
        variant (keyword (or variant-str "economical"))
        cfg     (merge (config/base-cfg) (variants variant))
        slug    (str "putc-" (name variant) "-" (subs (str (UUID/randomUUID)) 0 8))
        sid     (tenant/tenant-id->uuid slug)
        pool    (schema/create-pool {:base-cfg cfg})
        conn    (tenant/borrow pool slug)]
    (dom/ensure-user! conn "u0" "U0")
    (dotimes [i n] (dom/create-issue! conn {:title (str "I" i) :reporter "u0"}))
    (println "STOREID" (str sid))
    (tenant/close-all! pool)
    (shutdown-agents)))
