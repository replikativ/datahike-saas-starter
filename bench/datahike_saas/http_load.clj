(ns datahike-saas.http-load
  "End-to-end HTTP load test: fires requests at a running service (see
   datahike-saas.example.app) so the measured latency includes the web layer — Jetty,
   muuntaja/JSON, routing — on top of the domain work. Compare against the
   domain-level `workload mixed` numbers to see the HTTP overhead.

   Start a server first:  SAAS_TIER=tier1 PORT=8899 clj -M:run
   Then:  clj -M:bench -m datahike-saas.http-load '{:base \"http://localhost:8899\"}'"
  (:require [datahike-saas.harness :as h]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.time Duration]))

(def ^HttpClient client
  (-> (HttpClient/newBuilder) (.connectTimeout (Duration/ofSeconds 5)) (.build)))

(defn- GET [url]
  (.send client (-> (HttpRequest/newBuilder (URI/create url)) (.GET) (.build))
         (HttpResponse$BodyHandlers/ofString)))

(defn- POST [url body]
  (.send client (-> (HttpRequest/newBuilder (URI/create url))
                    (.header "content-type" "application/json")
                    (.POST (HttpRequest$BodyPublishers/ofString ^String body)) (.build))
         (HttpResponse$BodyHandlers/ofString)))

(def ^:private prios ["issue.priority/low" "issue.priority/medium" "issue.priority/high" "issue.priority/urgent"])

(defn- issue-json [i]
  (format "{\"title\":\"Issue %d regression\",\"reporter\":\"u%d\",\"priority\":\"%s\",\"assignee\":\"u%d\",\"labels\":[\"bug\"]}"
          i (mod i 5) (rand-nth prios) (mod (inc i) 5)))

(defn run
  [{:keys [base tenants target-rate duration-s workers read-frac seed-issues]
    :or   {base "http://localhost:8899" tenants 20 target-rate 200
           duration-s 20 workers 48 read-frac 0.9 seed-issues 20}}]
  (let [slugs (mapv #(str "http-" %) (range tenants))]
    (println "  seeding" tenants "tenants x" seed-issues "issues via HTTP ...")
    (doseq [s slugs, i (range seed-issues)]
      (POST (str base "/t/" s "/issues") (issue-json i)))
    (println "  open-loop:" target-rate "req/s for" duration-s "s, read-frac" read-frac "->" base)
    (let [op  (fn []
                (let [s (rand-nth slugs)]
                  (if (< (rand) read-frac)
                    (do (case (int (rand-int 3))
                          0 (GET (str base "/t/" s "/issues"))
                          1 (GET (str base "/t/" s "/stats"))
                          2 (GET (str base "/t/" s "/search?q=regression")))
                        :read)
                    (do (POST (str base "/t/" s "/issues") (issue-json (rand-int 1000))) :write))))
          res (h/run-open-loop {:target-rate target-rate :duration-s duration-s
                                :workers workers :op-fn op})]
      (assoc res :base base :tenants tenants :target-rate target-rate :read-frac read-frac))))

(defn -main [& args]
  (let [opts (if (seq args) (edn/read-string (str/join " " args)) {})
        {:keys [by-tag achieved-rate completed elapsed-s target-rate read-frac tenants] :as res} (run opts)]
    (println (format "\nHTTP open-loop: %d tenants, target %d req/s, read-frac %.2f" tenants target-rate read-frac))
    (println (format "  achieved %.1f req/s (%d reqs in %.1fs)" achieved-rate completed elapsed-s))
    (println (format "  %-6s %8s %8s %8s %8s %8s" "op" "count" "p50ms" "p90ms" "p99ms" "p99.9ms"))
    (doseq [[tag s] (sort by-tag)]
      (println (format "  %-6s %8d %8.2f %8.2f %8.2f %8.2f"
                       (name tag) (:count s) (:p50 s) (:p90 s) (:p99 s) (:p999 s))))
    (shutdown-agents)))
