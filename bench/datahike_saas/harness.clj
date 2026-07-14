(ns datahike-saas.harness
  "Measurement primitives: HdrHistogram percentiles + an open-loop,
   coordinated-omission-aware load runner.

   Open-loop matters: a closed-loop runner (each worker waits for its previous
   op) throttles itself under load and hides the tail — exactly the metric the
   read-scaling tiers exist to improve. Here ops fire on a fixed schedule and
   latency is measured from the INTENDED send time, so queueing delay when the
   system can't keep up is counted, not omitted."
  (:import [org.HdrHistogram ConcurrentHistogram]
           [java.util.concurrent Executors TimeUnit]
           [java.util.concurrent.locks LockSupport]))

(def ^:const highest-ns (* 120 1000000000)) ; 120 s ceiling

(defn hist ^ConcurrentHistogram [] (ConcurrentHistogram. highest-ns 3))

(defn record! [^ConcurrentHistogram h ^long nanos]
  (.recordValue h (min highest-ns (max 1 nanos))))

(defn ms [nanos] (-> nanos double (/ 1e6)))

(defn summary
  "Percentile summary of a histogram, in milliseconds."
  [^ConcurrentHistogram h]
  (when (pos? (.getTotalCount h))
    {:count (.getTotalCount h)
     :min   (ms (.getMinValue h))
     :mean  (ms (.getMean h))
     :p50   (ms (.getValueAtPercentile h 50.0))
     :p90   (ms (.getValueAtPercentile h 90.0))
     :p99   (ms (.getValueAtPercentile h 99.0))
     :p999  (ms (.getValueAtPercentile h 99.9))
     :max   (ms (.getMaxValue h))}))

(defn time-ns
  "Run thunk, return [result elapsed-ns]."
  [f]
  (let [t0 (System/nanoTime) r (f)] [r (- (System/nanoTime) t0)]))

(defn run-open-loop
  "Fire `op-fn` at `target-rate` ops/s for `duration-s`, dispatched across
   `workers` threads. `op-fn` is a 0-arg fn returning a tag keyword (e.g. :read
   / :write) identifying which histogram to record into. Latency is measured
   from each op's intended (scheduled) start.

   Returns {:by-tag {tag summary} :submitted n :completed n
            :elapsed-s s :achieved-rate ops/s}."
  [{:keys [target-rate duration-s workers op-fn]
    :or   {workers 32}}]
  (let [pool      (Executors/newFixedThreadPool workers)
        hists     {:read (hist) :write (hist)}
        completed (java.util.concurrent.atomic.AtomicLong.)
        interval  (long (/ 1e9 (double target-rate)))
        start     (System/nanoTime)
        deadline  (+ start (long (* duration-s 1e9)))]
    (loop [i 0]
      (let [intended (+ start (* (long i) interval))]
        (when (< intended deadline)
          (let [now (System/nanoTime)]
            (when (< now intended) (LockSupport/parkNanos (- intended now))))
          (.execute pool
                    (fn []
                      (let [tag  (op-fn)
                            done (System/nanoTime)]
                        (record! (get hists tag (:read hists)) (- done intended))
                        (.incrementAndGet completed))))
          (recur (inc i)))))
    (.shutdown pool)
    (.awaitTermination pool 120 TimeUnit/SECONDS)
    (let [elapsed (/ (- (System/nanoTime) start) 1e9)
          n       (.get completed)]
      {:by-tag        (into {} (for [[t h] hists :when (pos? (.getTotalCount h))]
                                 [t (summary h)]))
       :submitted     n
       :completed     n
       :elapsed-s     elapsed
       :achieved-rate (/ n elapsed)})))
