(ns datahike-saas.example.app
  "Composition root: wire the issue-tracker domain onto the kernel and start the service.

   This is the whole integration surface — the file you copy when you build your own SaaS.
   The kernel gives you a bounded per-tenant pool, tier config, lifecycle (export/delete/
   clone) and the HTTP server; you supply exactly two things:

     :ensure-schema   how to install your schema on a tenant's first open  (example.schema)
     :routes-fn       your tenant-scoped routes                            (example.routes)

   `clj -M:run` starts this (see deps.edn)."
  (:require [datahike-saas.kernel.server :as server]
            [datahike-saas.kernel.config :as config]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.routes :as routes]
            [replikativ.logging :as log])
  (:gen-class))

(defn start!
  "Start the issue-tracker service on `:port` (default 8888)."
  [opts]
  (server/start! (merge {:ensure-schema schema/ensure-schema!
                         :routes-fn     routes/routes}
                        opts)))

(defn stop! [] (server/stop!))

(defn -main [& _]
  (let [port (Integer/parseInt (or (System/getenv "PORT") "8888"))]
    (start! {:port port})
    (log/info :saas/ready {:port port :tier (config/current-tier)})
    @(promise)))
