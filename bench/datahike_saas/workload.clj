(ns datahike-saas.workload
  "Issue-tracker load: a config-settings comparison (write amplification), an
   open-loop mixed read/write benchmark, and a GC reclamation demo.

   Run: clj -M:bench -m datahike-saas.workload <bench> [edn-opts]
     compare   — net stored-object growth across store-config variants (storage/GC)
     mixed     — open-loop read/write on the tier's default config
     gc        — storage reclaimed by d/gc-storage after churn
   For PUTs/commit (the cost/latency write metric) see bin/put-count."
  (:require [datahike.api :as d]
            [datahike-saas.kernel.config :as config]
            [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom]
            [datahike-saas.harness :as h]
            [konserve.core :as k]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import [java.util UUID]))

;; ── helpers ─────────────────────────────────────────────────────────────────

(defn- short-id [] (subs (str (UUID/randomUUID)) 0 8))

(defn- conn-store [conn] (:store @conn))

(defn object-count
  "Number of konserve objects backing a tenant (its share of the bucket)."
  [conn]
  (count (k/keys (conn-store conn) {:sync? true})))

(def ^:private priorities
  [:issue.priority/low :issue.priority/medium :issue.priority/high :issue.priority/urgent])
(def ^:private labels ["bug" "feature" "docs" "chore"])

(defn seed-actors! [conn]
  (dotimes [i 5] (dom/ensure-user! conn (str "u" i) (str "User " i)))
  (doseq [l labels] (dom/ensure-label! conn l "#888888")))

;; ── write mix: one commit per call, realistic distribution ──────────────────

(defn write-once!
  "Perform one write commit against `conn`; `ids` is an atom holding created
   issue ids (for comment/assign/close). Returns the op tag."
  [conn ids i]
  (let [existing @ids
        u (str "u" (mod i 5))]
    (case (int (mod i 10))
      (0 1 2 3 4 5) (let [{:keys [id]} (dom/create-issue!
                                        conn {:title    (str "Issue " i ": " (rand-nth labels) " regression")
                                              :body     "steps to reproduce ..."
                                              :reporter u
                                              :priority (rand-nth priorities)
                                              :assignee (str "u" (mod (inc i) 5))
                                              :labels   [(rand-nth labels)]})]
                      (swap! ids conj id))
      (6 7) (when (seq existing) (dom/comment! conn (rand-nth existing) u "looking into this"))
      8     (when (seq existing) (dom/assign! conn (rand-nth existing) u))
      9     (when (seq existing) (dom/close! conn (rand-nth existing))))
    :write))

(defn read-once!
  "Perform one read query against `conn`."
  [conn]
  (let [db @conn]
    (case (int (rand-int 5))
      0 (dom/open-issues db)
      1 (dom/by-priority db (rand-nth priorities))
      2 (dom/stats db)
      3 (dom/assigned-to db (str "u" (rand-int 5)))
      4 (dom/with-label db (rand-nth labels)))
    :read))

;; ── (1) config-variant net-object-growth comparison (storage/GC) ────────────

(def variants
  "Curated store-config variants — enough to explain each knob, not a full grid."
  {:economical       {:fuse-index-roots? true  :commit-graph? false :keep-history? false :index-config {:diff-buf-size 256}}
   :no-diff-buf      {:fuse-index-roots? true  :commit-graph? false :keep-history? false :index-config {:diff-buf-size 0}}
   :no-fusion        {:fuse-index-roots? false :commit-graph? false :keep-history? false :index-config {:diff-buf-size 256}}
   :with-commit-graph {:fuse-index-roots? true :commit-graph? true  :keep-history? false :index-config {:diff-buf-size 256}}
   :full-history     {:fuse-index-roots? true  :commit-graph? true  :keep-history? true  :index-config {:diff-buf-size 256}}})

(defn- variant-cfg [base variant]
  (merge base (get variants variant)))

(defn run-writes!
  "Apply `n` write commits to a fresh tenant under `cfg`; measure per-commit
   latency and the NET STORED-OBJECT GROWTH (delta in the konserve key-set).

   Note: this is a storage/GC metric, NOT PUTs/commit. A commit overwrites the
   branch-head at a fixed key (a PUT that adds no new key), so key-set growth
   can be < 1/commit even though every commit does >=1 PUT. For actual
   PUTs/commit (the cost/latency metric) see `bin/put-count`."
  [cfg n]
  (let [slug (str "wa-" (short-id))
        pool (schema/create-pool {:base-cfg cfg})
        conn (tenant/borrow pool slug)
        _    (seed-actors! conn)
        base (object-count conn)
        ids  (atom [])
        wh   (h/hist)]
    (dotimes [i n]
      (let [[_ ns] (h/time-ns #(write-once! conn ids i))]
        (h/record! wh ns)))
    (let [growth (- (object-count conn) base)]
      (tenant/close-all! pool)
      {:object-growth growth
       :growth-per-commit (double (/ growth n))
       :write-ms (h/summary wh)})))

(defn compare-configs
  [{:keys [commits variant-keys] :or {commits 300 variant-keys (keys variants)}}]
  (let [base (config/base-cfg)]
    (mapv (fn [v]
            (println "  variant" v "...")
            (assoc (run-writes! (variant-cfg base v) commits) :variant v))
          variant-keys)))

;; ── (2) open-loop mixed read/write across many concurrent tenants ────────────

(defn- zipf-sampler
  "Return a 0-arg fn sampling a tenant index in [0,n) with a Zipf(s) weight — a
   few hot tenants and a long, mostly-idle tail, which is how real SaaS traffic
   looks. s = 0 is uniform (every tenant equally active)."
  [n s]
  (if (zero? (double s))
    #(rand-int n)
    (let [ws  (mapv #(/ 1.0 (Math/pow (inc %) (double s))) (range n))
          tot (reduce + ws)
          cum (vec (reductions + (map #(/ % tot) ws)))]
      (fn [] (let [r (rand)]
               (loop [i 0] (if (or (= i (dec n)) (<= r (cum i))) i (recur (inc i)))))))))

(defn mixed-load
  "Open-loop read/write spread across `tenants` databases, each kept hot (its own
   connection). Every op is routed to a tenant chosen by `skew` (0 = uniform;
   ~1.0 = Zipf, a hot set + idle tail). This is the multi-tenant scenario: many
   tenants served CONCURRENTLY — `workers` threads hit different tenants at once,
   reads come from each tenant's in-memory cache, and writes to the *same* tenant
   serialize through that tenant's single writer (writes to *different* tenants
   are fully parallel)."
  [{:keys [tenants target-rate duration-s workers read-frac seed-issues skew]
    :or   {tenants 20 target-rate 200 duration-s 20 workers 48 read-frac 0.9 seed-issues 30 skew 0}}]
  (let [pool  (schema/create-pool)
        slugs (mapv #(str "ml-" %) (range tenants))
        pick  (zipf-sampler tenants skew)]
    (println "  seeding" tenants "tenants x" seed-issues "issues (all connections hot) ...")
    (doseq [s slugs]
      (let [c (tenant/borrow pool s) ids (atom [])]
        (seed-actors! c)
        (dotimes [i seed-issues] (write-once! c ids i))))
    (println "  open-loop:" target-rate "ops/s for" duration-s "s, read-frac" read-frac
             "skew" skew)
    (let [op  (fn []
                (let [c (tenant/borrow pool (slugs (pick)))]
                  (if (< (rand) read-frac) (read-once! c) (write-once! c (atom []) (rand-int 6)))))
          res (h/run-open-loop {:target-rate target-rate :duration-s duration-s
                                :workers workers :op-fn op})]
      (tenant/close-all! pool)
      (assoc res :tenants tenants :target-rate target-rate :read-frac read-frac :skew skew))))

;; ── (3) GC reclamation ──────────────────────────────────────────────────────

(defn gc-demo
  "Churn a tenant to generate superseded (unreachable) index nodes, then reclaim
   them with d/gc-storage. Runs under a garbage-generating config by default
   (:no-diff-buf) — diff-buf buffers churn in memory, so it produces little
   garbage to collect in the first place (which is itself the point)."
  [{:keys [issues churn variant] :or {issues 50 churn 800 variant :no-diff-buf}}]
  (let [cfg  (variant-cfg (config/base-cfg) variant)
        pool (schema/create-pool {:base-cfg cfg})
        conn (tenant/borrow pool (str "gc-" (short-id)))
        ids  (atom [])]
    (seed-actors! conn)
    (dotimes [i issues] (swap! ids conj (:id (dom/create-issue! conn {:title (str "Issue " i) :reporter (str "u" (mod i 5))}))))
    (println "  churning" churn "updates under" variant "to generate superseded nodes ...")
    (dotimes [i churn] (dom/assign! conn (rand-nth @ids) (str "u" (mod i 5))))
    (let [before      (object-count conn)
          ;; d/gc-storage returns a throwable-promise — deref to await completion.
          [_ gc-ns]   (h/time-ns #(deref (d/gc-storage conn)))
          after       (object-count conn)]
      (tenant/close-all! pool)
      {:variant variant :issues issues :churn churn
       :objects-before before :objects-after after
       :reclaimed (- before after)
       :reclaimed-pct (double (* 100 (/ (- before after) (max 1 before))))
       :gc-ms (h/ms gc-ns)})))

;; ── (4) scale — many tenants on one node ────────────────────────────────────

(defn scale
  "Open `tenants` tenant databases (each seeded with `issues-per` issues) on one node and
   report per-tenant heap (all connections hot), cold-connect latency, and seed throughput.
   This is the db-per-tenant density headline.

   Runs with an UNBOUNDED pool on purpose: the point is to measure MB per HOT tenant, so
   every connection has to stay open. A bounded pool (the default in production —
   `SAAS_MAX_HOT`) would cap the count and this would measure the cap instead."
  [{:keys [tenants issues-per] :or {tenants 1000 issues-per 5}}]
  (let [pool (schema/create-pool {:max-hot nil})
        rt   (Runtime/getRuntime)
        _    (dotimes [_ 3] (System/gc))
        heap0 (- (.totalMemory rt) (.freeMemory rt))
        ch   (h/hist)
        t0   (System/nanoTime)]
    (dotimes [i tenants]
      (let [[c ns] (h/time-ns #(tenant/borrow pool (str "scale-" i)))
            ids    (atom [])]
        (h/record! ch ns)
        (dotimes [j issues-per] (write-once! c ids j))
        (when (zero? (mod (inc i) 200)) (println "   " (inc i) "tenants"))))
    (let [seed-s (/ (- (System/nanoTime) t0) 1e9)
          _      (dotimes [_ 3] (System/gc))
          heap1  (- (.totalMemory rt) (.freeMemory rt))
          objs   (object-count (tenant/borrow pool "scale-0"))]
      (tenant/close-all! pool)
      {:tenants tenants :issues-per issues-per
       :heap-mb-per-tenant (/ (- heap1 heap0) tenants 1048576.0)
       :total-heap-mb (/ heap1 1048576.0)
       :connect-ms (h/summary ch)
       :seed-s seed-s :tenants-per-s (/ tenants seed-s)
       :objects-per-tenant objs})))

;; ── reporting ───────────────────────────────────────────────────────────────

(defn- fmt [x] (if (number? x) (format "%.2f" (double x)) (str x)))

(defn print-compare [rows]
  (println "\nNet stored-object growth (storage/GC metric — NOT PUTs/commit; see bin/put-count):")
  (println (format "  %-18s %8s %11s %9s %9s %9s" "variant" "growth" "grow/commit" "w-p50ms" "w-p99ms" "w-mean"))
  (doseq [{:keys [variant object-growth growth-per-commit write-ms]} rows]
    (println (format "  %-18s %8d %11s %9s %9s %9s"
                     (name variant) object-growth (fmt growth-per-commit)
                     (fmt (:p50 write-ms)) (fmt (:p99 write-ms)) (fmt (:mean write-ms))))))

(defn print-mixed [{:keys [by-tag achieved-rate target-rate completed elapsed-s read-frac tenants skew]}]
  (println (format "\nMixed open-loop: %d tenants (skew %.1f), target %d ops/s, read-frac %.2f"
                   tenants (double (or skew 0)) target-rate read-frac))
  (println (format "  achieved %.1f ops/s (%d ops in %.1fs)" achieved-rate completed elapsed-s))
  (println (format "  %-6s %8s %8s %8s %8s %8s" "op" "count" "p50ms" "p90ms" "p99ms" "p99.9ms"))
  (doseq [[tag s] (sort by-tag)]
    (println (format "  %-6s %8d %8s %8s %8s %8s"
                     (name tag) (:count s) (fmt (:p50 s)) (fmt (:p90 s)) (fmt (:p99 s)) (fmt (:p999 s))))))

;; ── entrypoint ──────────────────────────────────────────────────────────────

(defn -main [& args]
  (let [[bench & more] args
        opts (if (seq more) (edn/read-string (str/join " " more)) {})]
    (println "== bench:" bench "tier:" (config/current-tier) "opts:" opts)
    (case bench
      "compare" (let [rows (compare-configs opts)]
                  (print-compare rows)
                  (clojure.pprint/pprint rows))
      "mixed"   (let [res (mixed-load opts)]
                  (print-mixed res)
                  (clojure.pprint/pprint (dissoc res :by-tag)))
      "gc"      (let [res (gc-demo opts)]
                  (clojure.pprint/pprint res))
      "scale"   (let [res (scale opts)]
                  (println (format "\n%d tenants on one node: %.2f MB/tenant (%.0f MB total), cold-connect p50 %.1fms/p99 %.1fms, seed %.0f tenants/s"
                                   (:tenants res) (:heap-mb-per-tenant res) (:total-heap-mb res)
                                   (:p50 (:connect-ms res)) (:p99 (:connect-ms res)) (:tenants-per-s res)))
                  (clojure.pprint/pprint res))
      (println "unknown bench; use: compare | mixed | gc | scale"))
    (shutdown-agents)))
