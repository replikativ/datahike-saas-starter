(ns datahike-saas.tier4-demo
  "Tier 4: a per-reader LMDB cache tier over the shared S3 bounds the cold-read tail.

   The topology (doc/ladder.md):

   - The **writer** holds the S3 backend DIRECTLY. It is not tiered: a local cache belongs
     on the readers, not on the single writer.
   - The **reader** is tiered `{lmdb, shared-s3}` with `:write-policy :frontend-only` —
     it caches locally and NEVER writes the shared, writer-owned S3.

   `:write-policy :frontend-only` is load-bearing when you measure this. Omit it and
   konserve defaults to `:write-through`: the writer's own commits then populate the LMDB,
   and the 'cold' scan is served by a cache it got for free — local disk vs network, not
   Tier 4. (Under `:frontend-only`, seeding THROUGH the tiered store would write nothing
   to S3 at all.)

   So a reader's LMDB starts EMPTY and warms two ways: konserve-sync pushes each
   commit's nodes as they are written (the live path), and read-through fills whatever
   the walk missed. This demo isolates the read-through half:

     scan 1 (cold LMDB)  — reads fall through to S3, and populate the cache on the way
     scan 2 (warm LMDB)  — a FRESH process: cold in-memory cache, warm LMDB. The payoff.
     s3                  — the same scan with no local tier at all: the tail we removed

   Runs in SEPARATE JVMs (one LMDB env per process, cold in-memory cache per scan) —
   orchestrate with `bin/tier4-demo`, or by hand:

     docker compose --profile tier1 up -d
     bin/latency-proxy 40                                    # S3 backend at +40ms
     export SAAS_SID=$(uuidgen) LMDB_PATH=~/tmp/t4-$SAAS_SID
     MINIO_PORT=9000 clj -M:bench:lmdb -m datahike-saas.tier4-demo seed
     MINIO_PORT=9000 clj -M:bench:lmdb -m datahike-saas.tier4-demo tiered-cold
     MINIO_PORT=9000 clj -M:bench:lmdb -m datahike-saas.tier4-demo tiered-warm
     MINIO_PORT=9000 clj -M:bench:lmdb -m datahike-saas.tier4-demo s3"
  (:require [datahike.api :as d]
            [datahike-lmdb.core]                 ;; registers :lmdb
            [konserve-s3.core]                   ;; registers :s3
            [datahike-saas.harness :as h])
  (:import [java.util UUID]))

(defn- sid [] (UUID/fromString (System/getenv "SAAS_SID")))
(defn- fe  [] (or (System/getenv "LMDB_PATH")
                  (str (System/getProperty "user.home") "/tmp/dh-tests/tier4-fe")))
(defn- s3 [port]
  {:backend :s3 :id (sid) :bucket "tenants" :region "us-east-1"
   :access-key "minioadmin" :secret "minioadmin" :path-style-access? true
   :endpoint-override {:protocol :http :hostname "localhost" :port port}})

(def ^:private knobs
  {:index :datahike.index/persistent-set :keep-history? false :schema-flexibility :write
   :fuse-index-roots? true :commit-graph? false :index-config {:diff-buf-size 256}})

(defn- reader-tiered
  "The Tier-4 READER store: local LMDB over the shared S3, cache-only.
   `:write-policy :frontend-only` is what makes it a cache and not a replica — without
   it konserve defaults to :write-through and the reader would write the writer's S3."
  [port]
  (merge knobs {:store {:backend :tiered :id (sid)
                        :write-policy :frontend-only
                        :read-policy  :frontend-first
                        :frontend-config {:backend :lmdb :path (fe) :id (sid)}
                        :backend-config  (s3 port)}}))

(defn- writer-s3
  "The Tier-4 WRITER store: the shared S3, held directly. No local tier."
  [port]
  (merge knobs {:store (s3 port)}))

(defn- scan [db] (count (d/q '[:find ?e ?t :where [?e :note/title ?t]] db)))

(defn- cold-scan [cfg]
  (let [c (d/connect cfg)]
    (try (let [[rows ns] (h/time-ns #(scan @c))] {:rows rows :ms (h/ms ns)})
         (finally (d/release c)))))

(defn -main [& [phase]]
  (let [n    (Long/parseLong (or (System/getenv "N") "5000"))
        port (Long/parseLong (or (System/getenv "PROXY_PORT") "19000"))]
    (case phase
      ;; WRITER: writes the shared S3 directly. The reader's LMDB is untouched.
      "seed"
      (let [cfg (writer-s3 port)]
        (d/create-database cfg)
        (let [c (d/connect cfg)]
          (d/transact c [{:db/ident :note/title :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}])
          (doseq [b (partition-all 500 (range n))]
            (d/transact c (mapv (fn [i] {:note/title (str "note-" i)}) b)))
          (d/release c))
        (println (format "SEEDED %d notes (writer -> shared S3; reader LMDB still EMPTY)" n)))

      ;; READER, first contact: LMDB empty. Reads fall through to S3 and fill the cache.
      "tiered-cold"
      (let [{:keys [rows ms]} (cold-scan (reader-tiered port))]
        (println (format "TIERCOLD %.0f %d" ms rows)))

      ;; READER, fresh process: in-memory cache cold, LMDB now warm. The Tier-4 payoff.
      "tiered-warm"
      (let [{:keys [rows ms]} (cold-scan (reader-tiered port))]
        (println (format "TIERWARM %.0f %d" ms rows)))

      ;; Baseline: no local tier at all.
      "s3"
      (let [{:keys [rows ms]} (cold-scan (writer-s3 port))]
        (println (format "S3ONLY %.0f %d" ms rows)))

      (println "usage: tier4-demo seed|tiered-cold|tiered-warm|s3  (SAAS_SID + LMDB_PATH required)"))
    (shutdown-agents)))
