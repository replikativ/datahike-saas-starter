(ns user
  "REPL entry point. `(go)` opens a tenant pool on the active tier and seeds a
   demo tenant; then poke at `datahike-saas.example.domain`.

       docker compose --profile tier1 up -d      # once
       clj -M:dev                                # nREPL on :7888
       (go) (seed! \"acme\") (open-issues \"acme\")

   Tenant lifecycle — the db-per-tenant payoff:

       (export \"acme\") (clone! \"acme\" \"acme-staging\") (delete! \"acme\")

   Blob attachments — :db.type/store-ref (see doc/blobs.md):

       (def bid (attach! \"acme\" (:first-issue (seed! \"acme\")) \"README.md\"))
       (String. (fetch \"acme\" bid))            ; the bytes back
       (attachments-live \"acme\")               ; ids the GC will keep"
  (:require [datahike-saas.kernel.config :as config]
            [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.kernel.lifecycle :as lc]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom]
            [datahike-saas.example.attachments :as att]
            [clojure.java.io :as io]
            [datahike.api :as d]))

(defonce pool (atom nil))

(defn go []
  (when-not @pool (reset! pool (schema/create-pool)))   ;; kernel pool + issue-tracker schema
  (let [tier  (config/current-tier)
        store (get-in (:base-cfg @pool) [:store :backend])]
    (cond-> {:tier tier :store store}
      (= :local tier)
      (assoc :note (str "local files under data/ — no Docker, no account. "
                        "For the real object-store path: docker compose --profile tier1 up -d "
                        "&& SAAS_TIER=tier1. For a real bucket: SAAS_TIER=tier2 + creds.")))))

(defn stop []
  (when @pool (tenant/close-all! @pool) (reset! pool nil)))

(defn conn [slug] (tenant/borrow @pool slug))

(defn seed!
  "Seed a tenant with a handful of users, labels, and issues."
  [slug]
  (let [c (conn slug)]
    (dom/ensure-user! c "alice" "Alice Ng")
    (dom/ensure-user! c "bob"   "Bob Ito")
    (dom/ensure-label! c "bug"     "#d73a4a")
    (dom/ensure-label! c "feature" "#0e8a16")
    (let [{:keys [id]} (dom/create-issue! c {:title "Login flow rejects valid tokens"
                                             :body "Repro: expired refresh token 500s"
                                             :reporter "alice" :assignee "bob"
                                             :priority :issue.priority/urgent
                                             :labels ["bug"]})]
      (dom/comment! c id "bob" "Looks like a clock-skew check; on it.")
      (dom/create-issue! c {:title "Export issues to CSV"
                            :reporter "bob" :priority :issue.priority/low
                            :labels ["feature"]})
      (dom/create-issue! c {:title "Dark mode for the dashboard"
                            :reporter "alice" :labels ["feature"]})
      {:seeded slug :first-issue id})))

;; query shortcuts
(defn open-issues [slug] (dom/open-issues @(conn slug)))
(defn stats [slug]       (dom/stats @(conn slug)))
(defn search [slug q]    (dom/search @(conn slug) q))

;; tenant lifecycle — a tenant IS a database, so these are ordinary operations
(defn export
  "Every datom in the tenant, as [e a v tx added] vectors (see `export-edn` for takeout)."
  [slug] (lc/export-tenant @pool slug))

(defn export-edn [slug] (lc/export-edn @pool slug))

(defn clone!
  "Copy a tenant into a NEW one — a staging copy, or a support repro against real data."
  [from to] (lc/restore-tenant! @pool to (lc/export-tenant @pool from)))

(defn delete!
  "Delete the tenant's DATABASE. Offboarding, and GDPR erasure, in one call."
  [slug] (lc/delete-tenant! @pool slug))

;; blob attachments — :db.type/store-ref, bytes IN the tenant's store (see doc/blobs.md)
(defn attach!
  "Attach a file (path) or byte-array to an issue as an in-store blob; returns its content id."
  [slug issue-id file-or-bytes]
  (let [bytes (if (bytes? file-or-bytes)
                file-or-bytes
                (with-open [in (io/input-stream (io/file file-or-bytes))]
                  (.readAllBytes in)))]
    (att/attach! (conn slug) issue-id
                 {:filename (if (bytes? file-or-bytes) "blob.bin" (str file-or-bytes))
                  :content-type "application/octet-stream" :bytes bytes})))

(defn fetch [slug blob-id] (att/fetch (conn slug) blob-id))
(defn attachments-live [slug] (att/live-blob-ids @(conn slug)))
