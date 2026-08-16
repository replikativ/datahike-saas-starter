(ns datahike-saas.pool-test
  "The bounded pool: it must cap memory, and it must never close a connection someone
   is reading. The second is the whole difficulty — a time-based reaper would sometimes
   evict a slow query's connection underneath it."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike-saas.kernel.tenant :as tenant]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(defn- mem-pool [max-hot]
  ;; schema/create-pool = kernel pool + the issue-tracker schema injected as :ensure-schema.
  (schema/create-pool {:max-hot max-hot
                       :base-cfg {:value-caps :default :store {:backend :memory}
                                  :keep-history? false
                                  :schema-flexibility :write
                                  :index :datahike.index/persistent-set}}))

(deftest pool-is-bounded
  (testing "the pool never holds more than :max-hot connections"
    (let [pool (mem-pool 3)]
      (try
        (doseq [i (range 10)]
          (tenant/borrow pool (str "t" i)))
        (is (= 3 (tenant/hot-count pool))
            "10 tenants touched, only :max-hot stay open")
        (finally (tenant/close-all! pool))))))

(deftest eviction-is-lru
  (testing "the LEAST-recently-used tenant is the one evicted"
    (let [pool (mem-pool 2)]
      (try
        (tenant/borrow pool "old")
        (tenant/borrow pool "mid")
        (tenant/borrow pool "old")            ;; re-touch: "mid" is now the LRU
        (tenant/borrow pool "new")            ;; over the bound -> evict "mid"
        (is (= 2 (tenant/hot-count pool)))
        (is (contains? (set (keys (into {} (:tenants pool)))) "old")
            "the re-touched tenant survives")
        (is (not (contains? (set (keys (into {} (:tenants pool)))) "mid"))
            "the least-recently-used tenant is evicted")
        (finally (tenant/close-all! pool))))))

(deftest a-pinned-tenant-is-never-evicted
  (testing "a tenant being read is never closed, even when it is the LRU and over the bound"
    ;; This is the assertion that matters. Evicting a pinned tenant would release the store
    ;; out from under an in-flight query — the failure a time-based reaper cannot rule out.
    (let [pool (mem-pool 2)]
      (try
        (tenant/borrow pool "reader")               ;; oldest, so first in line to be evicted
        (tenant/pin! pool "reader")                 ;; ... but a request is reading it
        (dotimes [i 5] (tenant/borrow pool (str "other" i)))
        (is (contains? (set (keys (into {} (:tenants pool)))) "reader")
            "a pinned tenant survives eviction pressure")
        (is (<= (tenant/hot-count pool) 2)
            "the bound is still met — unpinned tenants are evicted instead")
        (tenant/unpin! pool "reader")
        (tenant/borrow pool "trigger")              ;; unpinned now -> it can go
        (is (not (contains? (set (keys (into {} (:tenants pool)))) "reader"))
            "once unpinned it is evictable again")
        (finally (tenant/close-all! pool)))))

  (testing "when EVERY tenant is pinned, we exceed :max-hot rather than break a live request"
    (let [pool (mem-pool 2)]
      (try
        (doseq [i (range 5)]
          (tenant/borrow pool (str "p" i))
          (tenant/pin! pool (str "p" i)))
        (is (= 5 (tenant/hot-count pool))
            "a live request always wins over the bound")
        (finally
          (doseq [i (range 5)] (tenant/unpin! pool (str "p" i)))
          (tenant/close-all! pool))))))

(deftest evicted-tenant-reopens-with-its-data
  (testing "eviction loses nothing — the next borrow reopens the tenant's database"
    (let [pool (mem-pool 1)]
      (try
        (let [c (tenant/borrow pool "acme")]
          (dom/ensure-user! c "alice" "Alice")
          (dom/create-issue! c {:title "Login rejects valid tokens" :reporter "alice"}))
        (tenant/borrow pool "other")                 ;; evicts "acme" (max-hot 1)
        (is (= 1 (tenant/hot-count pool)))
        (let [c (tenant/borrow pool "acme")]         ;; reopened from the store
          (is (= ["Login rejects valid tokens"]
                 (map :issue/title (dom/open-issues @c)))
              "an evicted tenant reopens with its data intact"))
        (finally (tenant/close-all! pool))))))

(deftest pinning-is-concurrent-safe
  (testing "eviction pressure while a tenant is pinned by another thread"
    (let [pool  (mem-pool 2)
          _     (tenant/borrow pool "hot")
          start (CountDownLatch. 1)
          done  (CountDownLatch. 1)
          ok    (atom nil)
          t (Thread. (fn []
                       (tenant/pin! pool "hot")
                       (try
                         (.countDown start)
                         (Thread/sleep 300)          ;; a slow query
                         (reset! ok (some? (dom/open-issues @(tenant/borrow pool "hot"))))
                         (finally
                           (tenant/unpin! pool "hot")
                           (.countDown done)))))]
      (try
        (.start t)
        (.await start 5 TimeUnit/SECONDS)
        (dotimes [i 20] (tenant/borrow pool (str "churn" i)))   ;; hammer the bound
        (.await done 10 TimeUnit/SECONDS)
        (is (true? @ok) "the pinned tenant's connection stayed usable under eviction pressure")
        (finally (tenant/close-all! pool))))))
