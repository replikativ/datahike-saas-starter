(ns datahike-saas.coldstart
  "Cold-start read economics: what a SHORT-LIVED reader pays to answer one query.

   Tiers 3 and 4 assume a long-lived process — a node that keeps a warm node cache
   (Tier 3) or a warm local LMDB (Tier 4). A lambda has neither: it opens a store,
   answers one request, and dies. This benchmark measures what that costs, and
   which of the candidate fixes actually moves it.

   The thing being measured is NOT request price. At $0.40/M, GETs are a rounding
   error (doc/cost-model.md) — 100 of them cost $0.00004. What a cold reader pays
   is ROUND TRIPS, and there are two kinds, which respond to completely different
   fixes:

     depth-serial    within one lookup: root -> branch -> leaf, each fetch
                     dependent on the last. Bounded by tree DEPTH (~3 at bf 512).
                     Fixed by pinning the interior, or by a bigger branching factor.
     breadth-serial  N independent lookups issued one after another. Bounded by
                     the WORKING SET. Fixed by concurrency — and nothing else.

   Telling them apart matters because they are wildly different sizes, and the
   cheap fix addresses the big one. `fanout` measures the breadth ceiling directly.

   Four strategies, and what each needs to exist:

     naive        today. datahike restores nodes one at a time, on demand.
     preload      pull the whole database into a :memory frontend at connect.
                  SHIPS TODAY — `ready-store :tiered` (datahike store.cljc) already
                  does exactly this, hardcoded to konserve's populate-missing-strategy.
     fanout       the same key set, fetched concurrently. Measures the ceiling a
                  batched konserve-s3 would reach; konserve-s3 implements neither
                  PMultiReadBackingStore nor PMultiWriteBackingStore today, so
                  `sync-keys-to-frontend` falls to its serial loop.
     live-only    preload AFTER d/gc-storage — i.e. what walk-sync-from-the-head
                  would fetch, versus what enumerate-the-bucket fetches.

   Requires the real object-store path (konserve-s3's IO instrumentation counts
   there, and only there):

     docker compose --profile tier1 up -d
     SAAS_TIER=tier1 clj -M:bench -m datahike-saas.coldstart compare

   For a realistic round trip rather than a localhost disk, put the latency proxy
   in front — this is what makes the serial/parallel gap visible at all:

     bin/latency-proxy 20
     MINIO_PORT=19000 SAAS_TIER=tier1 clj -M:bench -m datahike-saas.coldstart compare

   ── What it found (full numbers in doc/results/coldstart.edn) ──────────────

   1. Concurrency is the whole game. Same 197 objects at +20ms: 5843ms serial,
      357ms at width 64 — 16.4x, for a change that alters nothing about WHAT is
      fetched. On localhost (no injected latency) the same spread is 6.3x, so the
      gap WIDENS with real object-store latency rather than narrowing.

   2. Preload-everything is 8-16x slower than naive today, and it is what datahike
      already does for a :tiered store. At 400 issues: naive 736ms/21 GETs vs
      11836ms/392 GETs to connect. The query afterwards really is free (1ms, 0
      GETs) — the cost is all in getting there, serially, twice over
      (connect-gets ~= 2x objects, because perform-sync enumerates with -keys and
      then re-fetches every key).

   3. 80% of what a full preload fetches is garbage (197 objects -> 40 after
      d/gc-storage). A walk from the branch head would never have asked for it.

   4. Small tenants are already optimal and need none of this: at 5 issues the
      fused db record IS the database — 1 GET, 0 node reads. The per-tenant model
      is what makes that true, and it is worth saying out loud that the cold-start
      problem is a big-single-database problem."
  (:require [datahike.api :as d]
            [datahike-saas.kernel.config :as config]
            [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom]
            [konserve.core :as k]
            [konserve-s3.core :as s3]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import [java.util UUID]
           [java.util.concurrent Executors ExecutorService Callable TimeUnit]))

;; ── measurement ─────────────────────────────────────────────────────────────

(defn- short-id [] (subs (str (UUID/randomUUID)) 0 8))

(defn- ms [nanos] (/ (double nanos) 1e6))

(defmacro timed
  "{:ms .. :result ..} for body."
  [& body]
  `(let [t0# (System/nanoTime)
         r#  (do ~@body)]
     {:ms (ms (- (System/nanoTime) t0#)) :result r#}))

(defn- gets
  "GET count out of a konserve-s3 io-stats summary. `:get` is the node-read op;
   `:get-etag` and `:head` are branch-head bookkeeping and counted separately so a
   node-fetch number is never inflated by them."
  [stats]
  (get-in stats [:get :n] 0))

(defmacro probed
  "Run body under konserve-s3's cross-thread IO accumulator. Cross-thread matters:
   the parallel arms below fetch on a pool, and datahike's :self writer commits on
   its own thread — a dynamic binding would miss both.

   Returns {:ms .. :gets .. :stats .. :result ..}."
  [& body]
  `(let [t0#  (System/nanoTime)
         out# (s3/with-global-io-stats ~@body)
         el#  (ms (- (System/nanoTime) t0#))]
     {:ms el# :gets (gets (:stats out#)) :stats (:stats out#) :result (:result out#)}))

(defn- require-s3! []
  (let [b (get-in (config/base-cfg) [:store :backend])]
    (when-not (= :s3 b)
      (println)
      (println "  This benchmark needs the object-store path; the active store is" b ".")
      (println "  Run:  docker compose --profile tier1 up -d")
      (println "        SAAS_TIER=tier1 clj -M:bench -m datahike-saas.coldstart" "<cmd>")
      (println)
      (System/exit 1))))

;; ── fixtures ────────────────────────────────────────────────────────────────

(def ^:private priorities
  [:issue.priority/low :issue.priority/medium :issue.priority/high :issue.priority/urgent])
(def ^:private labels ["bug" "feature" "docs" "chore"])

(defn seed-tenant!
  "A fresh tenant with `n` issues, then CLOSE the pool — every reader below must
   open its own connection, because a borrowed one carries a warm node cache and
   would measure nothing."
  [n]
  (let [slug  (str "cs-" (short-id))
        ;; :migrations nil — norms are not on the measured path (readers connect with
        ;; d/connect, not through the pool); they only add noise to the seed.
        pool  (schema/create-pool {:migrations nil})
        conn  (tenant/borrow pool slug)]
    (dotimes [i 5] (dom/ensure-user! conn (str "u" i) (str "User " i)))
    (doseq [l labels] (dom/ensure-label! conn l "#888888"))
    (dotimes [i n]
      (dom/create-issue! conn {:title    (str "Issue " i ": " (rand-nth labels) " regression")
                               :body     "steps to reproduce ..."
                               :reporter (str "u" (mod i 5))
                               :priority (rand-nth priorities)
                               :assignee (str "u" (mod (inc i) 5))
                               :labels   [(rand-nth labels)]}))
    (tenant/close-all! pool)
    slug))

(defn- reader-cfg
  "Config for a reader opening `slug` directly — no pool, so no shared cache and
   no schema/migration work on the measured path."
  [slug]
  (tenant/tenant-cfg {:base-cfg (config/base-cfg)} slug))

(defn- tiered-memory-cfg
  "The same tenant, but through a tiered {memory, s3} store. This is the
   `pull it all in at startup` proposal, and it needs no new code: datahike's
   `ready-store :tiered` runs konserve's populate-missing-strategy on connect."
  [slug]
  (let [cfg (reader-cfg slug)]
    (assoc cfg :store {:backend         :tiered
                       :id              (get-in cfg [:store :id])
                       :write-policy    :frontend-only
                       :read-policy     :frontend-first
                       :frontend-config {:backend :memory :id (get-in cfg [:store :id])}
                       :backend-config  (:store cfg)})))

(defn- query!
  "One representative read. `open-issues` walks the AVET tuple index and pulls each
   hit — the same shape as a tenant-scoped list endpoint."
  [db]
  (count (dom/open-issues db)))

;; ── store inspection ────────────────────────────────────────────────────────

(defn- store-of [conn] (:store @conn))

(defn store-inventory
  "Every key in the tenant's store, with its size. NB this is konserve's
   `list-keys`, which opens each blob to read its metadata — one GET per object,
   SERIALLY. That is not incidental: it is the same call `perform-sync` makes on
   both stores before it copies anything, so its cost IS the preload's floor."
  [store]
  (let [{:keys [ms result]} (timed (k/keys store {:sync? true}))]
    {:enumerate-ms ms
     :keys         (mapv :key result)
     :n            (count result)}))

;; ── strategies ──────────────────────────────────────────────────────────────

(defn cold-naive
  "Open the tenant cold and answer one query. What a lambda pays today.

   Connect and query are measured SEPARATELY, and both matter. Under
   `:fuse-index-roots? true` the index roots are inlined in the db record, so a
   small tenant's entire tree arrives with the connect's single GET and the query
   reads no nodes at all — the fused record IS the database. That is the Tier-3
   `1.00 GET/deref` result seen from the other side, and it is why the interesting
   sizes here are the ones where the tree outgrows its root."
  [slug]
  (let [c    (probed (d/connect (reader-cfg slug)))
        conn (:result c)]
    (try
      (let [q (probed (query! @conn))]
        {:connect {:ms (:ms c) :gets (:gets c)}
         :query   {:ms (:ms q) :gets (:gets q)}
         :ms      (+ (:ms c) (:ms q))
         :gets    (+ (:gets c) (:gets q))
         :answer  (:result q)})
      (finally (d/release conn)))))

(defn cold-preload
  "Open through a tiered {memory, s3} store — connect pulls the whole store into
   memory — then answer the same query. Reported separately, because the whole
   question is whether the connect cost is worth the query being free."
  [slug]
  (let [c    (probed (d/connect (tiered-memory-cfg slug)))
        conn (:result c)]
    (try
      (let [q (probed (query! @conn))]
        {:connect {:ms (:ms c) :gets (:gets c)}
         :query   {:ms (:ms q) :gets (:gets q)}
         :answer  (:result q)})
      (finally (d/release conn)))))

(defn cold-warmed
  "Open cold, run a budget-bounded BFS warm, then answer the same query.

   Three phases, each counted separately, because the whole question is whether
   the warm's cost is repaid by the query's saving. `:store-cache-size` is raised
   to hold the budget — leaving it at the default 64 would fetch the warm and then
   evict it, which `d/warm-db` clamps against rather than doing silently."
  [slug {:keys [depth budget width cache-size]
         :or   {depth :with-leaves budget 2000 width 64 cache-size 8192}}]
  (let [cfg  (assoc (reader-cfg slug) :store-cache-size cache-size)
        c    (probed (d/connect cfg))
        conn (:result c)]
    (try
      (let [w (probed (d/warm-db @conn {:depth depth :budget budget :width width}))
            q (probed (query! @conn))]
        {:connect {:ms (:ms c) :gets (:gets c)}
         :warm    {:ms (:ms w) :gets (:gets w) :report (:result w)}
         :query   {:ms (:ms q) :gets (:gets q)}
         :total   {:ms (+ (:ms c) (:ms w) (:ms q))
                   :gets (+ (:gets c) (:gets w) (:gets q))}})
      (finally (d/release conn)))))

(defn- fetch-all!
  "Fetch `ks` from `store` with `width` concurrent in-flight reads. width 1 is the
   serial loop konserve's sync-keys-to-frontend runs today for a backing that
   declares no multi-key support — which is every S3 backing."
  [store ks width]
  (if (= 1 width)
    (timed (doseq [key ks] (k/get store key nil {:sync? true})))
    (let [^ExecutorService pool (Executors/newFixedThreadPool (int width))]
      (try
        (timed (->> ks
                    (mapv (fn [key]
                            (reify Callable
                              (call [_] (k/get store key nil {:sync? true})))))
                    (.invokeAll pool)
                    (mapv #(.get ^java.util.concurrent.Future %))))
        (finally
          (.shutdown pool)
          (.awaitTermination pool 60 TimeUnit/SECONDS))))))

(defn fanout
  "The breadth ceiling: the SAME key set, fetched at increasing concurrency.

   This is the measurement that decides whether konserve-s3 needs batched reads.
   Nothing here changes what is fetched — only how many are in flight — so the
   spread between width 1 and width N is pure serialization, recoverable for free."
  [slug widths]
  (let [conn (d/connect (reader-cfg slug))]
    (try
      (let [store (store-of conn)
            {:keys [keys n enumerate-ms]} (store-inventory store)]
        {:objects   n
         :enumerate {:ms enumerate-ms :note "konserve list-keys: 1 serial GET/object"}
         :widths    (vec (for [w widths]
                           (let [p (probed (fetch-all! store keys w))]
                             {:width w
                              :ms    (:ms p)
                              :gets  (:gets p)
                              :per-op-ms (when (pos? n) (/ (:ms p) n))})))})
      (finally (d/release conn)))))

;; ── benchmarks ──────────────────────────────────────────────────────────────

(defn shape
  "Read amplification: total objects in the store vs nodes a single query touches.

   This is the number that decides whether `preload the whole database` is a good
   idea or a catastrophe. It is a good idea exactly when the total is within a
   small multiple of the working set — true for a per-tenant database, false for
   one big shared one."
  [sizes]
  (println "\n── shape: store size vs working set ──────────────────────────────")
  (printf "%8s %10s %12s %16s%n" "issues" "objects" "query GETs" "amplification")
  (doseq [n sizes]
    (let [slug (seed-tenant! n)
          conn (d/connect (reader-cfg slug))
          {:keys [keys]} (store-inventory (store-of conn))
          _     (d/release conn)
          cold  (cold-naive slug)
          ws    (:gets cold)]
      (printf "%8d %10d %12d %16s%n"
              n (count keys) ws
              (if (pos? ws)
                (format "%.1fx" (/ (double (count keys)) ws))
                "whole db in 1 GET")))))

(defn cold
  "Naive cold read at several tenant sizes: the baseline every fix is measured against."
  [sizes]
  (println "\n── cold: one query, cold connection, no cache ────────────────────")
  (printf "%8s | %10s %6s | %10s %6s | %8s %9s%n"
          "issues" "connect ms" "GETs" "query ms" "GETs" "total ms" "total GETs")
  (doseq [n sizes]
    (let [slug (seed-tenant! n)
          r    (cold-naive slug)]
      (printf "%8d | %10.1f %6d | %10.1f %6d | %8.1f %9d%n"
              n
              (get-in r [:connect :ms]) (get-in r [:connect :gets])
              (get-in r [:query :ms])   (get-in r [:query :gets])
              (:ms r) (:gets r)))))

(defn preload
  "Naive vs preload-everything, at several tenant sizes."
  [sizes]
  (println "\n── preload: tiered {memory, s3}, full sync on connect ────────────")
  (printf "%8s | %10s %8s | %10s %8s %10s %8s%n"
          "issues" "naive ms" "GETs" "connect ms" "GETs" "query ms" "GETs")
  (doseq [n sizes]
    (let [slug (seed-tenant! n)
          nv   (cold-naive slug)
          pl   (cold-preload slug)]
      (printf "%8d | %10.1f %8d | %10.1f %8d %10.1f %8d%n"
              n (:ms nv) (:gets nv)
              (get-in pl [:connect :ms]) (get-in pl [:connect :gets])
              (get-in pl [:query :ms])   (get-in pl [:query :gets])))))

(defn live-only
  "What walk-sync-from-the-head would save over enumerate-the-bucket: the same
   preload, before and after d/gc-storage. The delta is garbage that a
   reachability walk would never have fetched."
  [n]
  (println "\n── live-only: preload cost before vs after GC ────────────────────")
  (let [slug (seed-tenant! n)
        before (let [conn (d/connect (reader-cfg slug))
                     inv  (store-inventory (store-of conn))]
                 (d/release conn) inv)
        _      (let [conn (d/connect (reader-cfg slug))]
                 (try @(d/gc-storage conn) (finally (d/release conn))))
        after  (let [conn (d/connect (reader-cfg slug))
                     inv  (store-inventory (store-of conn))]
                 (d/release conn) inv)]
    (printf "  objects before GC : %d%n" (:n before))
    (printf "  objects after  GC : %d%n" (:n after))
    (printf "  garbage           : %d (%.0f%% of what a full preload fetches)%n"
            (- (:n before) (:n after))
            (if (pos? (:n before))
              (* 100.0 (/ (double (- (:n before) (:n after))) (:n before)))
              0.0))))

(defn fanout-bench
  "The breadth ceiling at one tenant size."
  [n widths]
  (println "\n── fanout: same key set, increasing concurrency ──────────────────")
  (let [slug (seed-tenant! n)
        r    (fanout slug widths)]
    (printf "  objects: %d   list-keys (serial, 1 GET/object): %.1f ms%n"
            (:objects r) (get-in r [:enumerate :ms]))
    (printf "%8s %10s %10s %12s %10s%n" "width" "ms" "GETs" "ms/object" "speedup")
    (let [base (:ms (first (:widths r)))]
      (doseq [{:keys [width ms gets per-op-ms]} (:widths r)]
        (printf "%8d %10.1f %10d %12.2f %10s%n"
                width ms gets (or per-op-ms 0.0)
                (if (pos? ms) (format "%.1fx" (/ base ms)) "-"))))))

(defn warm-bench
  "Naive vs BFS-warmed, across tenant sizes and warm policies.

   Two properties this is here to demonstrate, both of which are claims the design
   rests on:

     1. A budget at or above the store's object count leaves the query with ZERO
        GETs — `preload everything` is not a separate mode, it is what this loop
        does when it runs out of frontier before it runs out of budget.
     2. Nothing is discontinuous across the size x budget sweep. A mode switch
        would show up here as a step, and a step is a latency cliff waiting for
        the tenant that grows into it."
  [sizes]
  (println "\n── warm: budget-bounded BFS before the query ─────────────────────")
  (printf "%7s %-12s %6s | %8s %6s | %8s %6s | %8s %6s | %8s%n"
          "issues" "depth" "budget" "conn ms" "GETs" "warm ms" "GETs" "query ms" "GETs" "total ms")
  (doseq [n sizes
          [depth budget] [[:interior 2000] [:with-leaves 2000] [:with-leaves 8]]]
    (let [slug (seed-tenant! n)
          r    (cold-warmed slug {:depth depth :budget budget})]
      (printf "%7d %-12s %6d | %8.1f %6d | %8.1f %6d | %8.1f %6d | %8.1f%n"
              n (name depth) budget
              (get-in r [:connect :ms]) (get-in r [:connect :gets])
              (get-in r [:warm :ms])    (get-in r [:warm :gets])
              (get-in r [:query :ms])   (get-in r [:query :gets])
              (get-in r [:total :ms])))))

;; ── entry ───────────────────────────────────────────────────────────────────

(def ^:private default-sizes [5 25 100 400])
(def ^:private default-widths [1 4 16 64 128])

(defn -main [& [bench opts-str]]
  (require-s3!)
  (let [{:keys [sizes widths n]
         :or   {sizes default-sizes widths default-widths n 400}}
        (when opts-str (edn/read-string opts-str))]
    (case (or bench "compare")
      "shape"   (shape sizes)
      "cold"    (cold sizes)
      "preload" (preload sizes)
      "fanout"  (fanout-bench n widths)
      "warm"    (warm-bench sizes)
      "live"    (live-only n)
      "compare" (do (cold sizes)
                    (warm-bench sizes)
                    (preload sizes)
                    (fanout-bench n widths)
                    (live-only n))
      (println "unknown bench:" bench "— one of shape|cold|warm|preload|fanout|live|compare")))
  (println)
  (shutdown-agents))
