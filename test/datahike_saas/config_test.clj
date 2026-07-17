(ns datahike-saas.config-test
  "The tier profiles resolve to the expected store shapes — the config-swap spine."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike-saas.kernel.config :as config]))

(deftest tier1-is-minio-s3
  (let [{:keys [store] :as cfg} (config/base-cfg :tier1)]
    (is (= :s3 (:backend store)))
    (is (= "localhost" (get-in store [:endpoint-override :hostname])))
    (is (= 9000 (get-in store [:endpoint-override :port])))
    (testing "shared write-economy knobs are present on every tier"
      (is (true? (:fuse-index-roots? cfg)))
      (is (false? (:commit-graph? cfg)))
      (is (= 256 (get-in cfg [:index-config :diff-buf-size]))))))

(deftest tier2-drops-endpoint-and-nil-creds-without-env
  ;; With no S3_ENDPOINT/creds env, tier2 must be plain AWS: no endpoint-override,
  ;; nil creds stripped so the default credential chain applies.
  (let [{:keys [store]} (config/base-cfg :tier2)]
    (is (= :s3 (:backend store)))
    (is (nil? (:endpoint-override store)))
    (is (not (contains? store :s3-endpoint)))
    (is (not (contains? store :access-key)))))

(deftest tier4-is-tiered-lmdb-over-s3
  (let [{:keys [store]} (config/base-cfg :tier4)]
    (is (= :tiered (:backend store)))
    (is (= :lmdb (get-in store [:frontend-config :backend])))
    (is (= :s3 (get-in store [:backend-config :backend])))))

(deftest all-tiers-resolve
  (doseq [t config/tiers]
    (is (map? (config/base-cfg t)) (str t " resolves to a config map"))
    (is (some? (get-in (config/base-cfg t) [:store :backend])))))
