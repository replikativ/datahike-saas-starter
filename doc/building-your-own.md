# Building your own SaaS on the kernel

This template is a **kernel** (reusable db-per-tenant substrate) plus an **example** (an issue
tracker). To build your own product you keep `kernel/`, delete `example/`, and write four
small files. The kernel gives you, unchanged:

- a **bounded per-tenant connection pool** — one isolated Datahike database per tenant, LRU
  capped so heap tracks the *active* set, never evicting a connection mid-request
  (`kernel.tenant`);
- **tier config** — one node → real bucket → read replicas → streamed replicas, as a profile
  swap, never a code change (`kernel.config`, `resources/config.edn`);
- **lifecycle** — export / delete / clone a tenant, because a tenant *is* a database
  (`kernel.lifecycle`), exposed as `GET /t/:tenant/export` and `DELETE /t/:tenant`;
- the **HTTP server** — Jetty, JSON, role resolution (writer / reader / streaming),
  request-scoped pinning (`kernel.server`, `kernel.routes`).

## The contract

The kernel asks you for exactly two things, both passed to `server/start!`:

```clojure
(server/start! {:ensure-schema my.schema/ensure-schema!   ;; conn -> conn, run once per tenant on open
                :routes-fn     my.routes/routes            ;; conn-fn -> reitit route-data
                :migrations    "migrations"})              ;; resource dir, or nil
```

## The four files (copy `example/`, then edit)

1. **`schema.clj`** — your attributes, and an idempotent `ensure-schema!`. Also expose a
   `create-pool` one-liner so the REPL/benches get a pool with your schema pre-injected:
   ```clojure
   (defn ensure-schema! [conn]
     (when-not (schema-present? conn) (d/transact conn {:tx-data schema}))
     conn)
   (defn create-pool
     ([] (create-pool {}))
     ([opts] (tenant/create-pool (merge {:ensure-schema ensure-schema!} opts))))
   ```

2. **`domain.clj`** — your transactions and queries. **Tier-agnostic**: every fn takes a
   Datahike connection or db and never mentions the store. This is the file that does *not*
   change as you climb the scaling ladder.

3. **`routes.clj`** — your tenant-scoped routes, a fn `conn-fn -> route-data`. The kernel
   concatenates them after its own `/health` and lifecycle routes. Put your authn/authz in
   front of `conn-fn` (see the security note below).

4. **`app.clj`** — the composition root: hand the kernel your `ensure-schema!` and `routes`,
   and start. This is the whole integration surface — ~15 lines (see `example/app.clj`).

Optionally, a **`migrations/`** resource dir of EDN norms — applied lazily, per tenant, on
first open after a deploy. And, if you have files, an **`attachments.clj`** built on
`:db.type/store-ref` (see [`blobs.md`](blobs.md)).

## Two things the kernel deliberately does *not* do

- **Authentication.** The tenant slug is read straight from the URL path. What db-per-tenant
  buys you is *storage* isolation — a bug in your app can't leak across tenants because the
  slug selects a different **database**, not a `WHERE` clause. That is not a substitute for
  authenticating the caller. Resolve the caller's tenant from a verified token *in front of*
  `conn-fn`, and ignore (or cross-check) the path segment.
- **Cross-tenant queries.** By design there is no join across tenants — each is its own
  database. If you need fleet-wide analytics, export (`kernel.lifecycle`) into a warehouse;
  don't reach across databases at request time.

## Speak SQL instead?

If your domain is relational, [`pg-datahike`](https://github.com/replikativ/pg-datahike)
embeds a PostgreSQL-compatible adapter inside a Datahike process. Point it at the store
profiles here and you get db-per-tenant Postgres on object storage, scaling the same way —
your ORM and migrations unchanged.
