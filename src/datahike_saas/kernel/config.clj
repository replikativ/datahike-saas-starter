(ns datahike-saas.kernel.config
  "Load the tiered store config (resources/config.edn) for a given tier.

   KERNEL namespace — reusable across any db-per-tenant SaaS on Datahike; it knows
   nothing about the issue-tracker example.

   `resources/config.edn` is the single source of truth for the scaling story:
   one aero file whose `:store` profile is the only thing that differs between
   tiers. This namespace reads it and normalizes a couple of things aero can't
   express declaratively:

   - S3 endpoint: a bare `:s3-endpoint` URL (Tigris/Hetzner) is expanded into
     konserve-s3's `:endpoint-override` map; when unset (plain AWS) it is dropped
     so the default AWS endpoint is used.
   - nil credentials are stripped so the standard AWS credential chain applies."
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def tiers #{:local :tier1 :tier2 :tier3 :tier4})

(defn current-tier
  "The active tier, from SAAS_TIER. Defaults to :local — a plain directory on disk, so the
   repo runs with no Docker, no cloud account and no credentials. Everything that needs the
   real object-store path (benchmarks, CI, the trace-based counters) sets SAAS_TIER=tier1."
  []
  (let [t (some-> (System/getenv "SAAS_TIER") str/trim keyword)]
    (if (tiers t) t :local)))

(defn- expand-s3-endpoint
  "Turn a bare `:s3-endpoint` URL into konserve-s3's `:endpoint-override`, or drop
   it (and nil creds) so plain AWS with the default credential chain is used."
  [store]
  (if-not (= :s3 (:backend store))
    store
    (let [ep (:s3-endpoint store)
          store (dissoc store :s3-endpoint)
          store (cond-> store
                  (nil? (:access-key store)) (dissoc :access-key)
                  (nil? (:secret store))     (dissoc :secret))]
      (if (str/blank? (str ep))
        store
        (let [u (io/as-url ep)
              proto (keyword (.getProtocol u))
              port (let [p (.getPort u)]
                     (if (pos? p) p (if (= :https proto) 443 80)))]
          (assoc store :endpoint-override
                 {:protocol proto :hostname (.getHost u) :port port}))))))

(defn- normalize-store [store]
  (case (:backend store)
    :s3     (expand-s3-endpoint store)
    :tiered (-> store
                (update :backend-config expand-s3-endpoint))
    store))

(defn base-cfg
  "The shared Datahike config for the given tier — the value passed (with a
   per-tenant `:store :id`) to `d/create-database` / `d/connect`."
  ([] (base-cfg (current-tier)))
  ([tier]
   (let [cfg (aero/read-config (io/resource "config.edn") {:profile tier})]
     (-> cfg
         (dissoc :streaming)
         (update :store normalize-store)))))

(defn streaming
  "The read-scaling topology block for the given tier (nil for tier1/tier2)."
  ([] (streaming (current-tier)))
  ([tier]
   (:streaming (aero/read-config (io/resource "config.edn") {:profile tier}))))
