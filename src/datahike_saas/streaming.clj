(ns datahike-saas.streaming
  "Tier 4 — one authoritative writer STREAMS commits to read replicas that keep a
   local LMDB cache. (Tier 3 is the simpler, streaming-free tier: direct-S3 readers
   that just deref — see `datahike-saas.reader-demo`.)

   The writer holds each tenant's S3-backed Datahike connection and runs a kabel
   WebSocket server. A reader runs a kabel client peer and, per tenant, connects
   with the `:kabel` writer backend over a TIERED {lmdb, shared-s3} store: the
   stream delivers the branch head (fresh, no per-deref S3 read), konserve-sync
   pushes each commit's index nodes into the reader's LMDB, and node reads hit
   that local cache before falling back to the shared S3. So a read costs zero
   object-store round trips in the warm case, and readers scale horizontally.

   This uses datahike's kabel integration (loads from the released jar with the
   :kabel transport deps — kabel, konserve-sync, distributed-scope, http-kit)."
  (:require [datahike.api :as d]
            [datahike.kabel.connector]                  ;; registers -connect* :kabel
            [datahike.kabel.handlers :as handlers]
            [datahike.kabel.fressian-handlers :as fh]
            [datahike-saas.tenant :as tenant]
            [datahike-saas.config :as config]
            [kabel.peer :as peer]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [kabel.middleware.fressian :refer [fressian]]
            [konserve-sync.core :as sync]
            [is.simm.distributed-scope :as ds]
            [superv.async :refer [S <??]]
            [clojure.core.async :refer [<!!]]
            [konserve-s3.core]                          ;; registers :s3
            [replikativ.logging :as log])
  (:import [java.util UUID]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]))

;; Stable id for the single writer peer — readers dial this.
(def writer-peer-id #uuid "5aa50000-0000-0000-0000-000000000001")

(defn- dh-fressian [peer-config]
  (fressian (atom fh/read-handlers) (atom fh/write-handlers) peer-config))

;; ── Writer ───────────────────────────────────────────────────────────────────

(defn- writer-base-cfg
  "The store the WRITER holds. For a :tiered tier config (Tier 4) the writer uses the
   S3 backend directly — it's the shared source of truth; a local LMDB cache belongs on
   the readers, not the single writer. Plain stores pass through."
  []
  (let [cfg   (config/base-cfg)
        store (:store cfg)]
    (if (= :tiered (:backend store))
      (assoc cfg :store (:backend-config store))
      cfg)))

(defn start-writer!
  "Start the authoritative writer: a kabel server peer plus an S3-backed tenant
   pool. Returns a writer context. `ws-url` e.g. \"ws://0.0.0.0:8890\"."
  [{:keys [ws-url] :or {ws-url "ws://localhost:8890"}}]
  (let [pool    (tenant/create-pool {:base-cfg (writer-base-cfg)}) ;; the S3 backend
        handler (create-http-kit-handler! S ws-url writer-peer-id)
        srv     (peer/server-peer S handler writer-peer-id
                                  (comp (sync/server-middleware) ds/remote-middleware)
                                  dh-fressian)]
    (<?? S (peer/start srv))
    (ds/invoke-on-peer srv)
    (handlers/register-global-handlers! srv)
    (log/info :streaming/writer-started {:url ws-url})
    {:peer srv :pool pool :registered (ConcurrentHashMap.)}))

(defn writer-conn
  "Borrow a tenant connection on the writer, registering its store for remote
   access (once) so readers can sync and follow it."
  [{:keys [pool peer ^ConcurrentHashMap registered]} slug]
  (let [conn (tenant/borrow pool slug)]
    (.computeIfAbsent registered slug
                      (reify Function
                        (apply [_ _]
                          (handlers/register-store-for-remote-access!
                           (tenant/tenant-id->uuid slug) conn peer)
                          true)))
    conn))

(defn stop-writer! [{:keys [peer pool]}]
  (when peer (<?? S (peer/stop peer)))
  (when pool (tenant/close-all! pool)))

;; ── Reader ───────────────────────────────────────────────────────────────────

(defn start-reader!
  "Start a stateless read replica: a kabel client peer connected to the writer.
   Returns a reader context; open per-tenant following connections with
   `reader-conn`."
  [{:keys [ws-url] :or {ws-url "ws://localhost:8890"}}]
  (require 'datahike-lmdb.core)                          ;; register :lmdb for the tiered frontend
  (let [client (peer/client-peer S (UUID/randomUUID)
                                 (comp (sync/client-middleware) ds/remote-middleware)
                                 dh-fressian)]
    (ds/invoke-on-peer client)
    (<?? S (peer/connect S client ws-url))
    (log/info :streaming/reader-connected {:url ws-url})
    {:peer client :conns (ConcurrentHashMap.)}))

(defn reader-conn
  "Return this reader's following connection for a tenant, opening it on first use.
   The store is the tier's TIERED {lmdb, shared-s3} store (Tier 4): the kabel stream
   delivers the head (so `@conn` is fresh with no per-deref S3 read and no stale-head
   caching issue), while node reads come from the local LMDB cache falling back to the
   shared S3. The tenant must already be registered on the writer (`writer-conn`)."
  [{:keys [peer ^ConcurrentHashMap conns]} slug]
  (.computeIfAbsent conns slug
                    (reify Function
                      (apply [_ _]
                        (<!! (d/connect (-> (tenant/tenant-cfg {:base-cfg (config/base-cfg)} slug)
                                            (assoc :writer {:backend    :kabel
                                                            :peer-id    writer-peer-id
                                                            :local-peer peer}))
                                        {:sync? false}))))))

(defn stop-reader! [{:keys [peer ^ConcurrentHashMap conns]}]
  (doseq [[_ c] (into {} conns)] (try (d/release c) (catch Exception _)))
  (when peer (<?? S (peer/stop peer))))
