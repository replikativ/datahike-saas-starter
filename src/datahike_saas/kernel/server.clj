(ns datahike-saas.kernel.server
  "HTTP service kernel. Wires the tenant pool + reitit router + Jetty, and resolves the
   node's ROLE (single writer / Tier-3 direct reader / Tier-4 streaming) from SAAS_ROLE.

   KERNEL namespace — domain-agnostic. It knows nothing about the issue tracker: your
   domain's schema and routes are INJECTED via `start!`'s `:ensure-schema` and `:routes-fn`.
   The composition root that supplies them is `datahike-saas.example.app`.

   The whole service is tier-agnostic: it reads SAAS_TIER, builds the pool from
   that tier's config, and serves the same routes regardless of the store beneath."
  (:require [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.kernel.config :as config]
            [datahike-saas.kernel.routes :as kroutes]
            [clojure.string :as str]
            [reitit.ring :as ring]
            [reitit.ring.middleware.muuntaja :as muuntaja-mw]
            [reitit.ring.middleware.parameters :as parameters]
            [muuntaja.core :as m]
            [ring.adapter.jetty :as jetty]
            [replikativ.logging :as log]))

(def mtj
  "Muuntaja instance with a java.util.Date-aware JSON encoder."
  (m/create
   (assoc-in m/default-options
             [:formats "application/json" :encoder-opts :date-format]
             "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")))

(defn- read-only-mw
  "Reject writes on a read replica with a 405 that says where to send them.

   A Tier-3 reader's `:datahike-server` writer backend exists to make `@conn` re-read the
   branch head (it reports `:streaming? false`); it is NOT a working write path — it POSTs
   to datahike's own HTTP-server routes (`<url>/transact-writer`), which this template
   doesn't mount. Without this, a write to a reader dies with a confusing 404 from the
   writer's reitit app. Say the true thing instead."
  [writer-url]
  (fn [handler]
    (fn [req]
      (if (#{:post :put :patch :delete} (:request-method req))
        {:status 405
         :headers {"allow" "GET"}
         :body {:error  "read replica — writes are not served here"
                :writer writer-url
                :hint   "This node reads the shared bucket and auto-follows the writer. Send writes to the writer node."}}
        (handler req)))))

(defn- wrap-tenant-pin
  "PIN the request's tenant for the life of the request, so the LRU pool can never evict a
   connection that is being read.

   The pool bounds how many tenants stay hot (`SAAS_MAX_HOT`) and closes the least-recently-
   used beyond that — but closing a connection mid-query would release the store out from
   under an in-flight read. Pinning makes eviction safe without the handlers knowing about it:
   they keep taking a plain `conn-fn`, and this sits in the routing middleware where the
   `:tenant` path param is already resolved.

   A time-based grace window would be simpler and would occasionally evict a slow query's
   connection underneath it. This cannot."
  [pool]
  (fn [handler]
    (fn [req]
      (if-let [slug (get-in req [:path-params :tenant])]
        (do (tenant/pin! pool slug)
            (try (handler req)
                 (finally (tenant/unpin! pool slug))))
        (handler req)))))

(defn app
  "Build the ring handler. `routes-fn` is your domain's route contribution — a fn
   `conn-fn -> reitit-route-data` — concatenated after the kernel's own health/lifecycle
   routes (`kernel.routes`)."
  ([conn-fn] (app conn-fn {}))
  ([conn-fn {:keys [read-only? writer-url pool routes-fn] :or {routes-fn (constantly [])}}]
   (ring/ring-handler
    (ring/router
     (into (kroutes/kernel-routes pool) (routes-fn conn-fn))
     {:data {:muuntaja   mtj
             :middleware (cond-> [parameters/parameters-middleware
                                  muuntaja-mw/format-middleware]
                           pool       (conj (wrap-tenant-pin pool))
                           read-only? (conj (read-only-mw writer-url)))}})
    (ring/create-default-handler))))

(defn- conn-source
  "Build {:conn-fn (slug -> conn) :stop fn} for the node's role (SAAS_ROLE). `:ensure-schema`
   and `:migrations` are the injected domain schema installer / migrations resource, threaded
   into every tenant pool this node opens.
   - reader            : Tier 3 — direct-bucket pool with a NON-streaming writer backend,
                         so @conn re-reads the branch head each deref and follows the writer
   - writer-streaming  : Tier 4 — kabel server; streams each commit to readers
   - reader-streaming  : Tier 4 — kabel client on a tiered {lmdb, shared-bucket} store
   - else (unset)      : single node — :self writer, full read+write authority
   Streaming is resolved dynamically so the default path needs no kabel deps."
  [role {:keys [ensure-schema migrations] :or {ensure-schema identity migrations "migrations"}}]
  (case role
    ;; DIRECT reader (Tier 3): shared bucket + a NON-streaming writer backend.
    ;;
    ;; The `:datahike-server` writer is configured for ONE reason: it reports
    ;; `:streaming? false`, which is what makes `deref-conn` re-read the branch head from
    ;; the store on every `@conn`. That — not the absence of a writer — is what makes a
    ;; reader follow the writer. Leave it on the default `:self` writer (streaming) and
    ;; the node serves a FROZEN snapshot forever.
    ;;
    ;; It is NOT a working write path, and this node is read-only. `:datahike-server`
    ;; POSTs to `<url>/<op>-writer` — routes served by datahike's own HTTP server
    ;; (datahike.http.server), which this template does not mount; SAAS_WRITER_URL points
    ;; at the writer's *reitit* app, which has no such route. A write here would 404 from
    ;; a foreign API. So we mark the node read-only and say so with a 405 instead
    ;; (kernel.routes), and writes go to the writer node's own HTTP API.
    "reader"
    (let [base (assoc (config/base-cfg)
                      :writer (cond-> {:backend :datahike-server
                                       :url (or (System/getenv "SAAS_WRITER_URL") "http://localhost:8888")}
                                (System/getenv "SAAS_TOKEN") (assoc :token (System/getenv "SAAS_TOKEN"))))
          pool (tenant/create-pool {:base-cfg base :ensure-schema ensure-schema :migrations migrations})]
      {:conn-fn    (fn [slug] (tenant/borrow pool slug))
       :pool       pool
       :read-only? true
       :writer-url (or (System/getenv "SAAS_WRITER_URL") "http://localhost:8888")
       :stop       #(tenant/close-all! pool)})

    ;; Optional kabel streaming (lower staleness) — datahike-saas.kernel.streaming, loaded
    ;; dynamically so the default path needs no kabel deps.
    ("reader-streaming" "writer-streaming")
    (let [w?  (= role "writer-streaming")
          ns' (requiring-resolve (symbol "datahike-saas.kernel.streaming" (if w? "start-writer!" "start-reader!")))
          f   (requiring-resolve (symbol "datahike-saas.kernel.streaming" (if w? "writer-conn" "reader-conn")))
          st  (requiring-resolve (symbol "datahike-saas.kernel.streaming" (if w? "stop-writer!" "stop-reader!")))
          url (or (System/getenv "SAAS_WRITER_WS") (if w? "ws://0.0.0.0:8890" "ws://localhost:8890"))
          ctx (ns' {:ws-url url :ensure-schema ensure-schema :migrations migrations})]
      {:conn-fn (fn [slug] (f ctx slug)) :stop #(st ctx)})

    ;; default / "writer" / "single": :self writer — full read+write authority.
    (let [pool (tenant/create-pool {:ensure-schema ensure-schema :migrations migrations})]
      {:conn-fn (fn [slug] (tenant/borrow pool slug))
       :pool    pool
       :stop    #(tenant/close-all! pool)})))

(defonce ^:private state (atom nil))

(def ^:private known-roles #{"reader" "writer" "single" "writer-streaming" "reader-streaming"})

(defn- check-role!
  "Fail fast on the two role mistakes that are otherwise SILENT.

   1. An unknown SAAS_ROLE falls through to the default `:self` writer — so a typo
      ('reader-stream', 'replica') turns an intended read replica into a second WRITER
      on the shared bucket. Reject it.

   2. A node with no role is a `:self` writer. That is right for THE writer and
      catastrophic for a replica: `:self` reports `:streaming? true`, so `@conn` returns
      its in-memory atom instead of re-reading the branch head — the node serves a frozen
      snapshot forever, while also claiming write authority over a bucket someone else
      owns. We cannot tell which you meant, so we say so.

   The trigger is a SHARED store (:s3 / :tiered), not the tier — what matters is whether some
   OTHER process might be writing the same store, and the tier says nothing about that (the
   compose tier-3 profile runs SAAS_TIER=tier1 for both nodes). Declaring SAAS_ROLE=writer
   states your intent and silences it."
  [role store-backend]
  (when (and role (not (known-roles role)))
    (throw (ex-info (str "Unknown SAAS_ROLE " (pr-str role) ". Expected one of: "
                         (str/join ", " (sort known-roles))
                         ". An unrecognized role would silently start a :self WRITER.")
                    {:role role :known known-roles})))
  ;; `reader` is the TIER-3 role: it derefs the shared bucket, which works because its
  ;; :datahike-server writer is non-streaming so @conn re-reads the branch head. Point it at
  ;; a TIERED store and the branch head — a MUTABLE key — gets served from the local LMDB
  ;; cache (:read-policy :frontend-first), so the re-read returns the STALE cached head and
  ;; the node is frozen forever. Silently. That is why Tier 3 has no LMDB (doc/ladder.md) and
  ;; why Tier 4 delivers the head over the kabel STREAM instead of re-reading it.
  (when (and (= "reader" role) (= :tiered store-backend))
    (throw (ex-info (str "SAAS_ROLE=reader (Tier 3) cannot be used with a :tiered store (Tier 4). "
                         "A tiered reader caches the MUTABLE branch head in LMDB, so @conn re-reads "
                         "its own stale copy and the node serves a FROZEN snapshot forever. Tier 4 "
                         "readers get the head from the kabel stream: use SAAS_ROLE=reader-streaming.")
                    {:type :saas/tier3-reader-on-tiered-store :role role :store-backend store-backend})))
  (when (and (nil? role) (#{:s3 :tiered} store-backend))
    (log/warn :saas/no-role-on-shared-store
              {:store-backend store-backend
               :msg (str "No SAAS_ROLE set, and the store is SHARED (" (name store-backend) ") — "
                         "this node starts as a :self WRITER: full write authority, reading its "
                         "own in-memory db. Correct for a single node or THE writer (set "
                         "SAAS_ROLE=writer to say so and silence this). If it is meant to be a "
                         "read replica, set SAAS_ROLE=reader (Tier 3) or reader-streaming "
                         "(Tier 4) — a :self writer is *streaming*, so @conn never re-reads the "
                         "branch head and it would serve a FROZEN snapshot.")})))

(defn start!
  "Start the HTTP service. Options:
   - `:port`          Jetty port (default 8888).
   - `:ensure-schema` fn conn -> conn installing your domain schema on tenant open.
   - `:migrations`    migrations resource dir (default \"migrations\").
   - `:routes-fn`     fn conn-fn -> reitit-route-data — your domain's routes."
  ([] (start! {}))
  ([{:keys [port ensure-schema migrations routes-fn]
     :or {port 8888 ensure-schema identity migrations "migrations" routes-fn (constantly [])}}]
   (when @state (throw (ex-info "already running" {})))
   (let [role (System/getenv "SAAS_ROLE")
         _    (check-role! role (get-in (config/base-cfg) [:store :backend]))
         {:keys [conn-fn stop read-only? writer-url pool]}
         (conn-source role {:ensure-schema ensure-schema :migrations migrations})
         srv  (jetty/run-jetty (app conn-fn {:read-only? read-only? :writer-url writer-url
                                             :pool pool :routes-fn routes-fn})
                               {:port port :join? false})]
     (reset! state {:stop stop :server srv})
     (log/info :saas/started {:port port :tier (config/current-tier)
                              :role (or role "single") :read-only? (boolean read-only?)})
     @state)))

(defn stop! []
  (when-let [{:keys [stop server]} @state]
    (.stop server)
    (stop)
    (reset! state nil)))
