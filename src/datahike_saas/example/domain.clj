(ns datahike-saas.example.domain
  "Issue-tracker domain operations — transactions and queries.

   The DOMAIN layer of the example: this is what you replace with your own. Everything here
   is TIER-AGNOSTIC — it takes a Datahike connection and does not know or care whether the
   store underneath is MinIO, S3, a kabel-streamed replica, or an LMDB-over-S3 tier. That
   invariance is the whole point of the starter — moving up the scaling ladder never touches
   this file."
  (:require [datahike.api :as d]
            [clojure.string :as str])
  (:import [java.util Date UUID]))

;; ── Users & labels (upsert by unique identity) ──────────────────────────────

(defn ensure-user!
  "Upsert a user by handle; returns the handle."
  [conn handle name]
  (d/transact conn {:tx-data [{:user/handle handle :user/name name}]})
  handle)

(defn ensure-label!
  [conn name color]
  (d/transact conn {:tx-data [{:label/name name :label/color color}]})
  name)

;; ── Issue lifecycle ─────────────────────────────────────────────────────────

(defn- next-number
  "Next per-tenant issue number. Datahike serializes writes per connection, and
   the demo/bench workload has at most one writer per tenant, so read-max+1 is
   safe here; a highly-concurrent single tenant would want a transaction fn."
  [db]
  (inc (or (ffirst (d/q '[:find (max ?n) :where [_ :issue/number ?n]] db)) 0)))

(defn create-issue!
  "Create an issue. `m` supplies :title, :body, :reporter (handle), and optional
   :priority (keyword, default :medium), :assignee (handle), :labels (names).
   Returns {:id uuid :number n}."
  [conn {:keys [title body reporter priority assignee labels]}]
  (let [db     @conn
        id     (UUID/randomUUID)
        number (next-number db)
        now    (Date.)
        ;; Reporter/assignee/labels are upserted via nested maps (unique
        ;; identity), so an issue can be created without a separate seed step.
        tx     (cond-> {:issue/id         id
                        :issue/number     number
                        :issue/title      title
                        :issue/body       (or body "")
                        :issue/state      :issue.state/open
                        :issue/priority   (or priority :issue.priority/medium)
                        :issue/reporter   {:user/handle reporter}
                        :issue/created-at now
                        :issue/updated-at now}
                 assignee     (assoc :issue/assignee {:user/handle assignee})
                 (seq labels) (assoc :issue/labels (mapv (fn [n] {:label/name n}) labels)))]
    (d/transact conn {:tx-data [tx]})
    {:id id :number number}))

(defn comment!
  "Append a comment to an issue (by :issue/id)."
  [conn issue-id author body]
  (let [now (Date.)]
    (d/transact conn {:tx-data [{:db/id           [:issue/id issue-id]
                                 :issue/updated-at now
                                 :issue/comments  [{:comment/body       body
                                                    :comment/author     {:user/handle author}
                                                    :comment/created-at now}]}]})))

(defn assign!
  [conn issue-id assignee]
  (d/transact conn {:tx-data [{:db/id            [:issue/id issue-id]
                               :issue/assignee   {:user/handle assignee}
                               :issue/updated-at (Date.)}]}))

(defn set-state!
  "Set issue state to :issue.state/open or :issue.state/closed."
  [conn issue-id state]
  (d/transact conn {:tx-data [{:db/id            [:issue/id issue-id]
                               :issue/state      state
                               :issue/updated-at (Date.)}]}))

(defn close! [conn issue-id] (set-state! conn issue-id :issue.state/closed))

;; ── Queries ─────────────────────────────────────────────────────────────────

(def ^:private issue-pull
  [:issue/number :issue/title
   {:issue/state [:db/ident]}
   {:issue/priority [:db/ident]}
   {:issue/assignee [:user/handle]}
   {:issue/labels [:label/name]}])

(defn open-issues
  "Open issues, newest first, as pull maps."
  [db]
  (->> (d/q '[:find [(pull ?e pull) ...]
              :in $ pull
              :where [?e :issue/state :issue.state/open]]
            db issue-pull)
       (sort-by :issue/number >)))

(defn by-priority
  "Open issues at a given priority keyword (e.g. :issue.priority/urgent)."
  [db priority]
  (d/q '[:find [(pull ?e pull) ...]
         :in $ pull ?prio
         :where
         [?e :issue/state :issue.state/open]
         [?e :issue/priority ?prio]]
       db issue-pull priority))

(defn assigned-to
  "Open issues assigned to a user handle."
  [db handle]
  (d/q '[:find [(pull ?e pull) ...]
         :in $ pull ?handle
         :where
         [?u :user/handle ?handle]
         [?e :issue/assignee ?u]
         [?e :issue/state :issue.state/open]]
       db issue-pull handle))

(defn with-label
  "Issues carrying a given label name."
  [db label]
  (d/q '[:find [(pull ?e pull) ...]
         :in $ pull ?label
         :where
         [?l :label/name ?label]
         [?e :issue/labels ?l]]
       db issue-pull label))

(defn search
  "Case-insensitive substring search over issue titles."
  [db text]
  (d/q '[:find [(pull ?e pull) ...]
         :in $ pull ?needle
         :where
         [?e :issue/title ?t]
         [(clojure.string/lower-case ?t) ?lt]
         [(clojure.string/includes? ?lt ?needle)]]
       db issue-pull (str/lower-case text)))

(defn issue
  "Full detail for one issue id, including comments and attachments."
  [db id]
  (d/pull db
          [:issue/number :issue/title :issue/body
           {:issue/state [:db/ident]} {:issue/priority [:db/ident]}
           {:issue/reporter [:user/handle]} {:issue/assignee [:user/handle]}
           {:issue/labels [:label/name :label/color]}
           :issue/created-at :issue/updated-at
           {:issue/attachments [:attachment/blob :attachment/filename
                                :attachment/content-type :attachment/size :attachment/storage]}
           {:issue/comments [:comment/body {:comment/author [:user/handle]} :comment/created-at]}]
          [:issue/id id]))

(defn stats
  "Aggregate counts by state and priority — a small dashboard query."
  [db]
  {:by-state    (into {} (d/q '[:find ?s (count ?e)
                                :where [?e :issue/state ?st] [?st :db/ident ?s]]
                              db))
   :by-priority (into {} (d/q '[:find ?p (count ?e)
                                :where [?e :issue/priority ?pr] [?pr :db/ident ?p]]
                              db))})
