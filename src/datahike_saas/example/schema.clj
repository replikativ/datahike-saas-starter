(ns datahike-saas.example.schema
  "Per-tenant schema for the issue-tracker EXAMPLE.

   This is the domain layer — the part you replace to build your own SaaS. It depends on the
   kernel (`kernel.tenant`) to offer a ready-wired `create-pool`; the kernel never depends
   back on it.

   Each tenant is an isolated Datahike database with this schema. The domain is
   deliberately small but shows off the things that make Datahike a good fit for
   a multi-tenant SaaS: reference joins, cardinality-many, unique identities,
   component entities, and (optionally) history/time-travel — which the shipped profiles
   turn OFF (`:keep-history? false`).

   Note `domain/search` is a substring scan over titles, not a full-text index: Datahike
   supports secondary full-text indices, but this template does not use one."
  (:require [datahike.api :as d]
            [datahike-saas.kernel.tenant :as tenant]))

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

   ;; ── Attachments (blobs, out-of-line values — :db.type/store-ref) ───────────
   ;; A screenshot or crash dump on an issue. The value NAMES an object in the store; the
   ;; GC keeps that object alive while a datom points at it, and `delete!` erases it with
   ;; the tenant. Bytes live wherever you put them (the tenant's konserve store, or a
   ;; presigned S3 prefix) — see datahike-saas.example.attachments and doc/blobs.md.
   {:db/ident       :attachment/blob                ;; content id — the store handle
    :db/valueType   :db.type/store-ref
    :db/cardinality :db.cardinality/one}
   {:db/ident       :attachment/filename
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :attachment/content-type
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :attachment/size
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident       :attachment/storage           ;; :in-store | :s3-direct — where the bytes are
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident       :issue/attachments
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

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
  "Install the schema + enums on first connect (idempotent). This is the fn you hand the
   kernel as `:ensure-schema` — the kernel calls it once per tenant on open."
  [conn]
  (when-not (schema-present? conn)
    (d/transact conn {:tx-data (into schema enums)}))
  conn)

(defn create-pool
  "A tenant pool wired for the issue-tracker schema: the kernel pool with `ensure-schema!`
   injected. This is the one-liner a domain adds on top of the kernel — pass any extra
   `kernel.tenant/create-pool` options through."
  ([] (create-pool {}))
  ([opts] (tenant/create-pool (merge {:ensure-schema ensure-schema!} opts))))
