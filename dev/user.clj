(ns user
  "REPL entry point. `(go)` opens a tenant pool on the active tier and seeds a
   demo tenant; then poke at `datahike-saas.domain`.

       docker compose --profile tier1 up -d      # once
       clj -M:dev                                # nREPL on :7888
       (go) (seed! \"acme\") (open-issues \"acme\")

   Tenant lifecycle — the db-per-tenant payoff:

       (export \"acme\") (clone! \"acme\" \"acme-staging\") (delete! \"acme\")"
  (:require [datahike-saas.config :as config]
            [datahike-saas.tenant :as tenant]
            [datahike-saas.domain :as dom]
            [datahike-saas.lifecycle :as lc]
            [datahike.api :as d]))

(defonce pool (atom nil))

(defn go []
  (when-not @pool (reset! pool (tenant/create-pool)))
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
