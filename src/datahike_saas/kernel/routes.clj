(ns datahike-saas.kernel.routes
  "Domain-agnostic routes the kernel always provides: a health check and the
   tenant-LIFECYCLE endpoints (export / delete).

   KERNEL namespace. These operate on the DATABASE, not on your domain's rows, so they
   are identical for any db-per-tenant SaaS. Your domain's routes live beside them —
   `kernel.server/app` concatenates `kernel.routes/kernel-routes` with the `routes-fn`
   you inject (the issue-tracker example provides `example.routes/routes`).

   A tenant is a database, so offboarding is a DELETE and takeout is a GET."
  (:require [datahike-saas.kernel.lifecycle :as lc]))

(defn- ok [body] {:status 200 :body body})
(defn- not-found [] {:status 404 :body {:error "not found"}})

(defn- lifecycle-routes
  "Export / delete a tenant. They act on the DATABASE, so they need the pool rather than a
   single connection. A Tier-4 streaming reader has no pool and simply doesn't get them; a
   Tier-3 reader has one, but the read-only middleware (`server/read-only-mw`) still refuses
   its DELETE — deletion is the writer's job."
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

(defn kernel-routes
  "The always-on kernel routes: health, plus the lifecycle routes when a `pool` is given
   (a single writer/direct-reader node has one; a Tier-4 streaming reader does not)."
  [pool]
  (cond-> [["/health" {:get (fn [_] (ok {:status "ok"}))}]]
    pool (conj (lifecycle-routes pool))))
