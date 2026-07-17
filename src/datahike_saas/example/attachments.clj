(ns datahike-saas.example.attachments
  "Blob attachments on issues — the `:db.type/store-ref` demo.

   A store-ref is a datom value that NAMES an object (a screenshot, a crash dump, a PDF).
   The value adds exactly one thing over a plain uuid: the garbage collector MARKS it, so
   the object lives while a datom names it and becomes collectable once nothing does — and
   `lifecycle/delete-tenant!` erases the tenant's blobs with the tenant. The rule is *the
   database is the root set*. See doc/blobs.md and datahike's own doc/store-refs.md.

   Every fn here takes a plain Datahike CONNECTION (or db) — no pool, no tenant coupling —
   so it works from the REPL, an HTTP handler, or a background job alike. Where the bytes
   live is your choice, and this namespace shows BOTH deployments the one `:attachment/blob`
   type covers:

     IN-STORE     `attach!` / `fetch` — bytes in the tenant's own konserve store (k/bassoc /
                  k/bget). `d/gc-storage` reclaims them; portable across every backend;
                  `delete-tenant!` erases them. The bytes proxy through your JVM, so this is
                  for moderate sizes.

     S3-DIRECT    `record-external!` + `live-blob-ids` / `garbage-ids` — the browser PUTs
                  bytes straight to a presigned S3 URL (they never touch the JVM); you record
                  the id, and sweep your own prefix against the live set datahike computes.
                  This is the only shape that scales to large files."
  (:require [datahike.api :as d]
            [datahike.blob :as blob]
            [datahike.gc :as gc]
            [datahike.gc-guard :as guard]
            [konserve.core :as k]
            [clojure.java.io :as io]
            [superv.async :refer [S <??]])
  (:import [java.io ByteArrayOutputStream]))

(defn- store-id
  "The tenant store's id — what the GC keys its sweep by (`:id (:store config)`), and so
   what the write-window fence must name. Derived from the connection; no need to thread it."
  [conn]
  (get-in @conn [:config :store :id]))

;; ── In-store: bytes live in the tenant's own konserve store ─────────────────

(defn attach!
  "Attach `bytes` (a byte-array) to an issue, storing the bytes IN THE TENANT'S OWN STORE.

   `m` is {:filename :content-type :bytes}. Returns the attachment's content id (a uuid).

   THE WRITE WINDOW. The object exists in the store the instant `k/bassoc` returns, but
   nothing NAMES it until the following `d/transact` lands — and by the root-set rule an
   object nothing names is garbage. `guard/with-unreferenced-writes` holds a fence across
   both so a concurrent `d/gc-storage` in this process cannot reclaim it in the gap."
  [conn issue-id {:keys [filename content-type ^bytes bytes]}]
  (let [store (:store @conn)
        id    (blob/blob-id bytes)]              ;; content hash: write-once, dedup, as-of-safe
    (guard/with-unreferenced-writes (store-id conn)
      (k/bassoc store id bytes {:sync? true})
      (d/transact conn {:tx-data [{:db/id [:issue/id issue-id]
                                   :issue/attachments
                                   [{:attachment/blob         id
                                     :attachment/filename     filename
                                     :attachment/content-type content-type
                                     :attachment/size         (long (count bytes))
                                     :attachment/storage      :in-store}]}]}))
    id))

(defn fetch
  "Read an in-store attachment's bytes back by content id — returns a byte-array.

   konserve frames a binary value as [header][meta][payload], so `k/bget` hands the locked
   callback an `:input-stream` positioned at the payload; we copy it out inside the lock."
  [conn id]
  (k/bget (:store @conn) id
          (fn [{is :input-stream}]
            (let [out (ByteArrayOutputStream.)]
              (io/copy is out)
              (.toByteArray out)))
          {:sync? true}))

;; ── S3-direct: bytes never transit the JVM; you sweep the prefix yourself ────

(defn record-external!
  "Record an attachment whose bytes were uploaded STRAIGHT TO your object store — a presigned
   browser PUT to e.g. `s3://bucket/tenant/<slug>/blobs/<id>` — so they never touched the JVM.

   `m` is {:blob-id :filename :content-type :size}, where `:blob-id` is the content id you
   addressed the object under. Datahike marks this id as live but CANNOT delete external
   bytes: reclaim them yourself with `garbage-ids`."
  [conn issue-id {:keys [blob-id filename content-type size]}]
  (d/transact conn {:tx-data [{:db/id [:issue/id issue-id]
                               :issue/attachments
                               [{:attachment/blob         blob-id
                                 :attachment/filename     filename
                                 :attachment/content-type content-type
                                 :attachment/size         size
                                 :attachment/storage      :s3-direct}]}]})
  blob-id)

(defn live-blob-ids
  "The set of store-ref ids the database still names — the live set for sweeping an external
   prefix. Marks across ALL branches and retained history: datahike owns the hard half."
  [db]
  (<?? S (gc/reachable-store-refs db)))

(defn garbage-ids
  "Given the ids currently present under your external prefix, the ones safe to delete
   (named by no datom). List your prefix, subtract the live set, delete the rest."
  [db present-ids]
  (let [live (live-blob-ids db)]
    (into [] (remove live) present-ids)))
