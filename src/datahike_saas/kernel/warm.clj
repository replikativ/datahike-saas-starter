(ns datahike-saas.kernel.warm
  "Budget-bounded breadth-first index warm — pull a database's upper levels into the
   node cache in WAVES instead of discovering them one blocking round trip at a time.

   > ⚠️ **EXPERIMENTAL.** A prototype, and deliberately one: it belongs in datahike
   > (as `d/warm-index!` behind the index protocol), not in an application kernel.
   > It lives here because this repo is where the object-store read path gets
   > exercised — see `bench/datahike_saas/coldstart.clj` and doc/benchmarks.md §7.
   > Expect the name and the option map to move when it upstreams. The
   > ClojureScript arm is deliberately absent; see `## On ClojureScript` below.

   ## Why

   A cold reader's wall time is `misses x RTT`, with nothing overlapping: a scan
   asks for a node, blocks on the GET, and only then learns the next address.
   Measured in this repo at +20ms injected latency, the marginal cost of one more
   node was 24.96 ms against a ~25 ms round trip — one node, one round trip, zero
   overlap.

   It does not have to be that way, and the fix needs no prediction. A `Branch`
   holds `addresses()` — EVERY child address — the moment it is materialized. So
   the addresses of a whole level are known one level in advance. This walks
   breadth-first and fetches each level concurrently.

   ## The two bounds, and why there are two

   `:depth` bounds the SHAPE, `:budget` bounds the COST, and whichever binds
   first wins.

     :depth :interior     expand while (>= level 2) — children are branches, so
                          this stops exactly at the leaf boundary. Exact, not a
                          heuristic: leaves are level 0 and the root knows the
                          tree height.
     :depth :with-leaves  expand while (>= level 1) — everything.
     :depth <integer>     at most that many levels below the start node.

   Having both is what keeps this free of latency cliffs. There is no `preload
   everything` mode and no `warm the interior` mode to switch between — a small
   database with a large budget runs out of frontier and has fetched itself
   entirely; a large one hits the budget and stops. Same code, same config,
   continuous in database size. A mode switch is a cliff, and tenants grow.

   Measured (400 issues, +20ms): `:with-leaves` budget 8 warms 8 nodes and takes
   the query from 21 GETs to 12 — a partial warm buying a partial saving, landing
   between naive and fully-warmed with no step anywhere.

   ## Sizing a budget

   In a B-tree the interior is a geometric series, so `interior/total` is a
   constant fraction independent of database size — but size it from the MEASURED
   fill, not the branching factor. Nodes run about half full, so the effective
   fanout is ~bf/2 and the interior is ~`2/bf` of the tree, twice the naive
   estimate. Measured here at bf 32: 93 interior nodes of 1504 total (6.2%,
   against 1/32 = 3.1%).

   ## Selective warming

   `:from`/`:to` scope the walk to a key range: at every level only the child
   indices covering the range are expanded, so cost is proportional to the RANGE
   rather than to the database. That is how warming stays affordable on a store
   too large to warm whole.

   Prefer `warm-datoms!` / `warm-seek!` over passing `:from`/`:to` by hand. They
   take the same `[e a v tx]` components as `d/datoms` / `d/seek-datoms` and build
   their bounds with datahike's OWN `components->pattern`, so a warm and the scan
   it is warming for cannot disagree. Hand-built bounds can, silently: the pattern
   builder PERMUTES components per index (`:avet` reads `[a v e tx]` and produces
   `datom(v, a, e, tx)`) and resolves idents and lookup refs on the way. Get that
   wrong and you warm a valid-but-different subtree — no error, no wrong answer,
   just a warm that misses and a query that quietly pays full price.

   A per-tenant database is this same partitioning done physically — the tenant's
   store IS the covering subtree, which is why the tenants in this repo warm
   completely in one wave and never need the range at all.

   ## What it cannot do

   This bounds DEPTH. It does nothing for a caller that issues many independent
   scans one after another — that breadth lives above datahike, and no amount of
   prefetching below can see it. Such a caller has to issue its seeks
   concurrently (safe: nodes are immutable, the node cache is atom-based).

   ## On ClojureScript

   Not ported, and the port is not a port. cljs has no threads, but it does not
   need them — bounded `Promise.all` per level is simpler than this thread pool,
   and the BFS shape is natively async. Two things genuinely differ: `:width`
   cannot share a default (a browser gives ~6 connections per origin on HTTP/1.1,
   so 64 merely queues), and the value proposition inverts — datahike's cljs read
   path runs a sync query engine over async storage, so a complete warm is what
   makes synchronous querying FEASIBLE rather than merely fast. That is worth
   doing inside datahike, where `async+sync` already serves both runtimes from one
   source, and not worth faking here."
  (:require [replikativ.logging :as log]
            ;; INTERNAL datahike namespaces, knowingly. `components->pattern` is how
            ;; `d/datoms` itself builds slice bounds (datahike.db/contextual-datoms),
            ;; and reusing it is the whole point: a reimplementation of the per-index
            ;; component permutation is exactly how a warm and its scan drift apart.
            ;; The coupling disappears when this moves into datahike, where these are
            ;; neighbouring namespaces rather than someone else's internals.
            [datahike.db.utils :as dbu]
            [datahike.constants :as const]
            [datahike.datom :refer [datom]])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet ANode Branch IStorage]
           [java.util Comparator]
           [java.util.concurrent Executors ExecutorService Callable Future TimeUnit]))

(def ^:private default-width
  "Concurrent in-flight restores. 64 measured optimal against local MinIO (16.4x
   over serial at +20ms); 128 REGRESSED there. A real bucket tolerates far more —
   a starting point to measure from, not a constant to inherit."
  64)

(def ^:private default-budget 2000)

(def index-keys
  "Primary indices. The temporal twins exist only under :keep-history?."
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn- branch? [n] (instance? Branch n))

(defn- child-bounds
  "Inclusive child-index bounds of `node`, intersected with [from to] (nil =
   unbounded). `_keys[i]` is the MAX key of child i, so `searchFirst` — the first
   index whose key is >= the probe — names the child that could contain it, for
   both ends of the range."
  [^ANode node ^Comparator cmp from to]
  (let [n   (.len node)
        lst (dec n)]
    [(if from (min (max 0 (.searchFirst node from cmp)) lst) 0)
     (if to   (min (max 0 (.searchFirst node to cmp)) lst) lst)]))

(defn- expand?
  "Should this node's children be fetched? A leaf never has children, so it always
   terminates the walk regardless of policy — which is what makes `:interior` fall
   out of the loop rather than needing to be enforced."
  [^ANode node depth round]
  (let [lvl (.level node)]
    (cond
      (< lvl 1)              false
      (= :interior depth)    (>= lvl 2)
      (= :with-leaves depth) true
      (integer? depth)       (< round depth)
      :else                  false)))

(defn- round-robin
  "Fair interleave of unequal-length colls. Used to share one budget across
   indices: without it, whichever index is enumerated first eats the budget and a
   query against a later index gets nothing warmed."
  [colls]
  (lazy-seq
   (let [colls (remove empty? colls)]
     (when (seq colls)
       (concat (map first colls) (round-robin (map next colls)))))))

(defn- fetch-wave!
  "Restore `reqs` ({:addr :storage ..}) with at most `width` in flight.

   Concurrent `restore` is safe: PSS nodes are immutable and content/uuid-keyed,
   and datahike's CachedStorage caches through `clojure.core.cache.wrapped` (atom
   `swap!`). Two threads racing the same address duplicate a fetch — wasted work,
   never a wrong answer.

   A per-call pool rather than a shared one: nothing to own, nothing to shut down
   on a failure path, and a warm is not hot enough for the churn to matter."
  [reqs width]
  (let [^ExecutorService pool (Executors/newFixedThreadPool (int (max 1 width)))]
    (try
      (->> reqs
           (mapv (fn [{:keys [^IStorage storage addr]}]
                   (reify Callable (call [_] (.restore storage addr)))))
           (.invokeAll pool)
           (mapv (fn [^Future f] (.get f))))
      (finally
        (.shutdown pool)
        (.awaitTermination pool 120 TimeUnit/SECONDS)))))

(defn- warm-trees!
  "One breadth-first walk across SEVERAL trees, sharing one budget.

   Interleaved rather than sequential: at each round every tree contributes its
   next level, and the budget is spent round-robin across them. Warming eavt to
   exhaustion while avet gets nothing is the wrong answer for a query that reads
   avet, and which index a query needs is not knowable here."
  [entries {:keys [depth budget width] :or {depth :interior budget default-budget width default-width}}]
  (let [t0 (System/nanoTime)]
    (loop [frontier (vec entries)
           round    0
           left     (long budget)
           fetched  0
           by-level []
           per-idx  {}]
      (let [groups (->> frontier
                        (filter #(and (branch? (:node %)) (expand? (:node %) depth round)))
                        (group-by :index))
            reqs   (when (pos? left)
                     (vec (round-robin
                           (for [[_ es] groups]
                             (for [{:keys [^Branch node from to ^Comparator cmp] :as e} es
                                   :let  [[lo hi] (child-bounds node cmp from to)]
                                   i     (range lo (inc hi))
                                   :let  [a (.address node i)]
                                   ;; nil address = an in-memory child never stored.
                                   ;; Cannot happen on a freshly-connected cold tree;
                                   ;; skipped rather than trusted.
                                   :when (some? a)]
                               (assoc e :addr a :node nil))))))]
        (if (empty? reqs)
          {:fetched fetched :by-level by-level :rounds round :by-index per-idx
           :budget-left left :budget-exhausted? false
           :ms (/ (- (System/nanoTime) t0) 1e6)}
          (let [take-n (min (count reqs) left)
                batch  (subvec reqs 0 take-n)
                nodes  (fetch-wave! batch width)
                next-f (mapv (fn [n r] (assoc r :node n)) nodes batch)
                left'  (- left take-n)
                per-idx (reduce (fn [m {:keys [index]}] (update m index (fnil inc 0)))
                                per-idx batch)]
            (if (zero? left')
              {:fetched (+ fetched take-n) :by-level (conj by-level take-n)
               :rounds (inc round) :by-index per-idx
               :budget-left 0 :budget-exhausted? true
               :ms (/ (- (System/nanoTime) t0) 1e6)}
              (recur next-f (inc round) left' (+ fetched take-n)
                     (conj by-level take-n) per-idx))))))))

(defn- tree-entry
  "A walk root for one index, or nil when there is nothing to walk."
  [index ^PersistentSortedSet pset {:keys [from to]}]
  (let [storage (.-_storage pset)
        ;; `.root` restores from the stored address if it is not already in hand —
        ;; free under :fuse-index-roots?, where the root rides in the db record.
        root    (.root pset)]
    (when (and root storage)
      {:index index :node root :storage storage :cmp (.comparator pset)
       :from from :to to})))

(defn warm-index!
  "Breadth-first warm of ONE index (a PersistentSortedSet), into its node cache.

   Options: :depth (:interior | :with-leaves | integer) :budget :width :from :to.
   Returns {:fetched :by-level :rounds :height :budget-left :budget-exhausted? :ms}.
   `:by-level` and `:budget-exhausted?` are the point of the report: they make a
   decaying warm visible as a metric before it is visible in p99."
  ([pset] (warm-index! pset {}))
  ([^PersistentSortedSet pset opts]
   (if-let [e (tree-entry :index pset opts)]
     (assoc (warm-trees! [e] opts) :height (.level ^ANode (:node e)))
     {:fetched 0 :by-level [] :rounds 0 :height 0
      :budget-left (:budget opts default-budget) :budget-exhausted? false :ms 0.0})))

(defn warm-datoms!
  "Warm exactly the subtree that `(d/datoms db index-type & components)` will scan.

   `components` is the same `[e a v tx]` prefix `d/datoms` takes, in the index's
   own component order (`:avet` -> `[a v e tx]`), and may be shorter or empty.

   The bounds come from `dbu/components->pattern` — the SAME call
   `datahike.db/contextual-datoms` makes to build its `-slice` arguments, with the
   same `e0/tx0` and `emax/txmax` fills. That is the point of this function: warm
   and scan derive their range from one function, so they agree by construction
   rather than by the caller getting a permutation right.

   Returns `warm-index!`'s report, or a zero report if the index is absent."
  ([db index-type components] (warm-datoms! db index-type components {}))
  ([db index-type components opts]
   (let [pset (get db index-type)]
     (if-not (instance? PersistentSortedSet pset)
       {:fetched 0 :by-level [] :rounds 0 :height 0
        :budget-left (:budget opts default-budget) :budget-exhausted? false :ms 0.0}
       (warm-index! pset
                    (assoc opts
                           :from (dbu/components->pattern db index-type components
                                                          const/e0 const/tx0)
                           :to   (dbu/components->pattern db index-type components
                                                          const/emax const/txmax)))))))

(defn warm-seek!
  "Warm forward from a `seek-datoms` position — READAHEAD, not a bounded range.

   `d/seek-datoms` is asymmetric: its lower bound is the components pattern but its
   upper bound is `(datom emax nil nil txmax)`, i.e. the end of the index
   (datahike.db/contextual-seek-datoms). So there is no range to be proportional to
   here and `:budget` is the only thing bounding the work — which is the right
   shape for a cursor that will consume an unknown amount, and the wrong shape for
   one that will read a single head. Size the budget to what you expect to consume."
  ([db index-type components] (warm-seek! db index-type components {}))
  ([db index-type components opts]
   (let [pset (get db index-type)]
     (if-not (instance? PersistentSortedSet pset)
       {:fetched 0 :by-level [] :rounds 0 :height 0
        :budget-left (:budget opts default-budget) :budget-exhausted? false :ms 0.0}
       (warm-index! pset
                    (assoc opts
                           :from (dbu/components->pattern db index-type components
                                                          const/e0 const/tx0)
                           :to   (datom const/emax nil nil const/txmax)))))))

(defn warm-db!
  "Warm every present index of `db`, sharing one budget round-robin across them.

   Clamps the budget to the node cache: `:store-cache-size` is ENTRY-counted, so
   warming past it fetches nodes only to evict them. 0.8x leaves room for the
   query that follows to bring in its own leaves without evicting the spine."
  ([db] (warm-db! db {}))
  ([db {:keys [indices budget] :as opts}]
   (let [cache-size (get-in db [:config :store-cache-size])
         budget     (or budget default-budget)
         capped     (if (and cache-size (> budget (* 0.8 cache-size)))
                      (long (* 0.8 cache-size))
                      budget)
         _          (when (< capped budget)
                      (log/warn :warm/budget-clamped
                                {:requested budget :capped capped
                                 :store-cache-size cache-size
                                 :msg "budget exceeded 0.8x the entry-counted node cache; raise :store-cache-size to warm more"}))
         entries    (keep (fn [k]
                            (let [idx (get db k)]
                              (when (instance? PersistentSortedSet idx)
                                (tree-entry k idx opts))))
                          (or indices index-keys))]
     (assoc (warm-trees! entries (assoc opts :budget capped))
            :budget-clamped? (< capped budget)))))
