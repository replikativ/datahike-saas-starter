(ns datahike-saas.domain-test
  "Domain logic over an in-memory datahike db — no object store needed."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [datahike-saas.schema :as schema]
            [datahike-saas.domain :as dom]))

(def ^:dynamic *conn* nil)

(defn mem-conn-fixture [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? false}]
    (d/create-database cfg)
    (let [conn (schema/ensure-schema! (d/connect cfg))]
      (binding [*conn* conn] (f))
      (d/release conn)
      (d/delete-database cfg))))

(use-fixtures :each mem-conn-fixture)

(deftest create-and-query
  (let [c *conn*
        {:keys [id number]} (dom/create-issue!
                             c {:title "Login rejects valid tokens" :reporter "alice"
                                :assignee "bob" :priority :issue.priority/urgent :labels ["bug"]})]
    (testing "issue is created with number 1 and shows as open"
      (is (= 1 number))
      (is (= [1] (map :issue/number (dom/open-issues @c)))))
    (testing "referenced users/labels were upserted on first use"
      (is (= "bob" (get-in (dom/issue @c id) [:issue/assignee :user/handle])))
      (is (= #{"bug"} (set (map :label/name (:issue/labels (dom/issue @c id)))))))
    (testing "filters"
      (is (= [1] (map :issue/number (dom/by-priority @c :issue.priority/urgent))))
      (is (= [1] (map :issue/number (dom/assigned-to @c "bob"))))
      (is (= [1] (map :issue/number (dom/with-label @c "bug"))))
      (is (seq (dom/search @c "login"))))))

(deftest numbers-increment
  (let [c *conn*]
    (is (= 1 (:number (dom/create-issue! c {:title "a" :reporter "u"}))))
    (is (= 2 (:number (dom/create-issue! c {:title "b" :reporter "u"}))))
    (is (= 3 (:number (dom/create-issue! c {:title "c" :reporter "u"}))))))

(deftest comment-and-close
  (let [c *conn*
        {:keys [id]} (dom/create-issue! c {:title "bug" :reporter "alice"})]
    (dom/comment! c id "bob" "reproduced")
    (is (= 1 (count (:issue/comments (dom/issue @c id)))))
    (testing "closing removes it from open + updates stats"
      (dom/close! c id)
      (is (empty? (dom/open-issues @c)))
      (is (= {:issue.state/closed 1} (:by-state (dom/stats @c)))))))

(deftest stats-aggregate
  (let [c *conn*]
    (dom/create-issue! c {:title "a" :reporter "u" :priority :issue.priority/high})
    (dom/create-issue! c {:title "b" :reporter "u" :priority :issue.priority/high})
    (dom/create-issue! c {:title "c" :reporter "u" :priority :issue.priority/low})
    (is (= {:issue.state/open 3} (:by-state (dom/stats @c))))
    (is (= {:issue.priority/high 2 :issue.priority/low 1} (:by-priority (dom/stats @c))))))
