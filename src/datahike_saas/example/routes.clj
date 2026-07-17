(ns datahike-saas.example.routes
  "Tenant-scoped HTTP routes for the issue-tracker domain. JSON in/out.

   The DOMAIN's route contribution. `routes` is the fn you hand the kernel as `:routes-fn`
   (see `datahike-saas.example.app`); `kernel.server/app` concatenates it after the kernel's
   own health/lifecycle routes. It closes over a `conn-fn` (slug -> connection) — the only
   tier-aware seam: a single/Tier-1/2/3 node passes `#(tenant/borrow pool %)`, a Tier-4
   streaming reader passes `#(streaming/reader-conn ctx %)`. The handlers themselves are a
   thin shell over `datahike-saas.example.domain`.

   ⚠️ THERE IS NO AUTHENTICATION HERE. The tenant slug is read straight from the URL
   path, so anyone who knows a slug can read that tenant. This is a template: put your
   authn/authz in front of `conn-fn` — resolve the caller's tenant from a verified token
   and IGNORE the path segment, or reject the request when they disagree. The isolation
   this repo argues for is *storage* isolation (a bug in your app can't leak across
   tenants, because the slug selects a different DATABASE, not a WHERE clause) — it is
   not a substitute for authenticating the caller."
  (:require [datahike-saas.example.domain :as dom]
            [datahike-saas.example.attachments :as att])
  (:import [java.util UUID Base64]))

(defn- conn [conn-fn req] (conn-fn (get-in req [:path-params :tenant])))

(defn- ok [body] {:status 200 :body body})
(defn- created [body] {:status 201 :body body})
(defn- not-found [] {:status 404 :body {:error "not found"}})

(defn- b64-decode ^bytes [^String s] (.decode (Base64/getDecoder) s))
(defn- b64-encode ^String [^bytes b] (.encodeToString (Base64/getEncoder) b))

(defn routes
  "Reitit route data for the issue-tracker API, closing over `conn-fn` (slug -> conn)."
  [conn-fn]
  [["/t/:tenant"
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

    ;; In-store blob attachment (:db.type/store-ref). Bytes come base64 in the JSON body —
    ;; enough to demonstrate the round-trip over the API without multipart plumbing; for
    ;; large files use the presigned-S3-direct shape instead (see doc/blobs.md).
    ["/issues/:id/attachments"
     {:post (fn [req]
              (let [id (UUID/fromString (get-in req [:path-params :id]))
                    {:keys [filename content-type data]} (:body-params req)]
                (created {:blob-id (str (att/attach! (conn conn-fn req) id
                                                     {:filename filename
                                                      :content-type content-type
                                                      :bytes (b64-decode data)}))})))}]
    ["/attachments/:blob-id"
     {:get (fn [req]
             (let [bid (UUID/fromString (get-in req [:path-params :blob-id]))]
               (ok {:blob-id (str bid) :data (b64-encode (att/fetch (conn conn-fn req) bid))})))}]

    ["/stats" {:get (fn [req] (ok (dom/stats @(conn conn-fn req))))}]
    ["/search" {:get (fn [req] (ok {:results (dom/search @(conn conn-fn req)
                                                         (get-in req [:query-params "q"] ""))}))}]]])
