(ns datahike-saas.handlers
  "Tenant-scoped HTTP API over the issue-tracker domain. JSON in/out.

   Routes are `/t/:tenant/...` — the tenant slug selects the Datahike connection
   via a `conn-fn` (slug -> connection). That indirection is the only tier-aware
   seam: a Tier 1/2/3 node passes `#(tenant/borrow pool %)` (shared bucket; a Tier-3
   reader differs only in its non-streaming `:writer` backend), while a Tier-4
   streaming reader passes `#(streaming/reader-conn ctx %)` (tiered lmdb+s3,
   kabel-followed).
   The handlers themselves are a thin shell over `datahike-saas.domain`.

   ⚠️ THERE IS NO AUTHENTICATION HERE. The tenant slug is read straight from the URL
   path, so anyone who knows a slug can read that tenant. This is a template: put your
   authn/authz in front of `conn-fn` — resolve the caller's tenant from a verified token
   and IGNORE the path segment, or reject the request when they disagree. The isolation
   this repo argues for is *storage* isolation (a bug in your app can't leak across
   tenants, because the slug selects a different DATABASE, not a WHERE clause) — it is
   not a substitute for authenticating the caller."
  (:require [datahike-saas.domain :as dom]
            [datahike-saas.lifecycle :as lc])
  (:import [java.util UUID]))

(defn- conn [conn-fn req] (conn-fn (get-in req [:path-params :tenant])))

(defn- ok [body] {:status 200 :body body})
(defn- created [body] {:status 201 :body body})
(defn- not-found [] {:status 404 :body {:error "not found"}})

(declare base-routes lifecycle-routes)

(defn routes
  "Reitit route data for the tenant API, closing over `conn-fn` (slug -> conn).

   `opts` may carry `:pool` — the tenant registry. Given one, the tenant-LIFECYCLE routes
   are mounted (export / delete): they operate on the DATABASE, not on rows, so they need
   the pool rather than a single connection. A Tier-4 streaming reader has no pool and
   simply doesn't get them; a Tier-3 reader has one, but the read-only middleware
   (`core/read-only-mw`) still refuses its DELETE — deletion is the writer's job."
  ([conn-fn] (routes conn-fn {}))
  ([conn-fn {:keys [pool]}]
   (cond-> (base-routes conn-fn)
     pool (conj (lifecycle-routes pool)))))

(defn- lifecycle-routes
  "A tenant is a database, so offboarding is a DELETE and takeout is a GET."
  [pool]
  ["/t/:tenant"
   ["/export"
    {:get (fn [req]
            (let [slug (get-in req [:path-params :tenant])]
              {:status 200
               :headers {"content-type" "application/edn"
                         "content-disposition" (str "attachment; filename=\"" slug ".edn\"")}
               :body (lc/export-edn pool slug)}))}]
   [""
    {:delete (fn [req]
               (let [slug (get-in req [:path-params :tenant])]
                 ;; Deletes the tenant's DATABASE. Complete by construction: their data
                 ;; lives in no other tenant's database, so there is nowhere else to sweep.
                 (if (lc/delete-tenant! pool slug)
                   (ok {:deleted slug})
                   (not-found))))}]])

(defn- base-routes
  [conn-fn]
  [["/health" {:get (fn [_] (ok {:status "ok"}))}]

   ["/t/:tenant"
    ["/issues"
     {:get  (fn [req] (ok {:issues (dom/open-issues @(conn conn-fn req))}))
      :post (fn [req]
              (let [{:keys [title body reporter priority assignee labels]} (:body-params req)]
                (created (dom/create-issue!
                          (conn conn-fn req)
                          (cond-> {:title title :body body :reporter reporter}
                            priority (assoc :priority (keyword priority))
                            assignee (assoc :assignee assignee)
                            labels   (assoc :labels labels))))))}]

    ["/issues/:id"
     {:get (fn [req]
             (let [id (UUID/fromString (get-in req [:path-params :id]))
                   d  (dom/issue @(conn conn-fn req) id)]
               (if (:issue/number d) (ok d) (not-found))))}]

    ["/issues/:id/comments"
     {:post (fn [req]
              (let [id (UUID/fromString (get-in req [:path-params :id]))
                    {:keys [author body]} (:body-params req)]
                (dom/comment! (conn conn-fn req) id author body)
                (created {:ok true})))}]

    ["/issues/:id/close"
     {:post (fn [req]
              (let [id (UUID/fromString (get-in req [:path-params :id]))]
                (dom/close! (conn conn-fn req) id)
                (ok {:ok true})))}]

    ["/stats" {:get (fn [req] (ok (dom/stats @(conn conn-fn req))))}]
    ["/search" {:get (fn [req] (ok {:results (dom/search @(conn conn-fn req)
                                                         (get-in req [:query-params "q"] ""))}))}]]])
