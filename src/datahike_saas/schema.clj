(ns datahike-saas.schema
  "Per-tenant schema for the issue-tracker demo.

   Each tenant is an isolated Datahike database with this schema. The domain is
   deliberately small but shows off the things that make Datahike a good fit for
   a multi-tenant SaaS: reference joins, cardinality-many, unique identities,
   component entities, and (optionally) history/time-travel — which the shipped profiles
   turn OFF (`:keep-history? false`).

   Note `domain/search` is a substring scan over titles, not a full-text index: Datahike
   supports secondary full-text indices, but this template does not use one."
  (:require [datahike.api :as d]))

(def schema
  [;; ── Users ────────────────────────────────────────────────────────────────
   {:db/ident       :user/handle
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :user/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; ── Labels ───────────────────────────────────────────────────────────────
   {:db/ident       :label/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :label/color
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; ── Issues ───────────────────────────────────────────────────────────────
   {:db/ident       :issue/id
    :db/valueType   :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :issue/number
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :issue/title
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/body
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   ;; state/priority as idents (enum-style refs) — cheap to store, query by keyword.
   {:db/ident       :issue/state
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/priority
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/reporter
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/assignee
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/labels
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/ident       :issue/comments
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}
   {:db/ident       :issue/created-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/updated-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}

   ;; ── Comments (component of an issue) ──────────────────────────────────────
   {:db/ident       :comment/body
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :comment/author
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :comment/created-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}])

(def enums
  "Enum idents for issue state and priority — transacted once alongside the schema."
  [{:db/ident :issue.state/open}
   {:db/ident :issue.state/closed}
   {:db/ident :issue.priority/low}
   {:db/ident :issue.priority/medium}
   {:db/ident :issue.priority/high}
   {:db/ident :issue.priority/urgent}])

(defn schema-present? [conn]
  (boolean (seq (d/q '[:find ?e :where [?e :db/ident :issue/id]] @conn))))

(defn ensure-schema!
  "Install the schema + enums on first connect (idempotent)."
  [conn]
  (when-not (schema-present? conn)
    (d/transact conn {:tx-data (into schema enums)}))
  conn)
