# Migrations across N databases

This is the objection that kills db-per-tenant in architecture reviews, and it deserves a real
answer rather than a shrug. **"So I now run ten thousand migrations?"**

The short version: **you don't run them at all.** Each tenant migrates itself the first time it is
opened after a deploy. No maintenance window, no orchestrator, no big bang. The cost amortizes
into normal traffic, and a tenant that never wakes costs nothing.

But the objection is *earned*, so start with why.

## Why this reputation is deserved

The Rails ecosystem is the cautionary tale, and it is worth taking seriously.

- [At 5,000 tenants, a migration that takes 1 second per schema takes over an hour][rails] — and
  when it fails partway you are "left with multiple databases in different states."
- Postgres `max_connections` bites early. Apartment switches tenants with `SET search_path`, which
  is per-connection, so **PgBouncer in transaction mode breaks it** — you are stuck in session
  mode, which caps you sooner than you expect.
- Parallelising migrations means giving up advisory locks.
- The **Apartment maintainers no longer recommend the approach**, and 2026 Rails guidance is to use
  row-level tenancy and avoid "schema-migration purgatory."

[Turso][turso-dep], running db-per-tenant at the largest scale anyone does, **deprecated** their
parent→child schema push and moved to a [pull-based registry][turso-blog]: each database migrates
itself, at its own pace. Their words: orchestrating a million databases centrally is "a
coordination nightmare."

Note what those two independently converged on. **Centralised push is the thing that fails. Lazy,
per-database pull is the thing that works.** That is the design below.

## What it actually costs here

Measured (`bin/`-less, but reproducible: 50 tenants × 100 issues, MinIO, additive attribute):

| | |
|---|---|
| per tenant | **p50 31 ms** — one commit, 1 PUT |
| 50 tenants | 1.8 s |
| extrapolated, **single-threaded** | 1,000 → **31 s** · 10,000 → **5.2 min** |

That is **~32× cheaper per tenant** than the 1 s/tenant that makes Rails migrations an hour long,
for three structural reasons:

1. **Adding an attribute is not `ALTER TABLE`.** Datoms are sparse. There is no column to add to
   every row and no default to backfill — it is one small transaction that says the attribute
   exists. Old entities simply lack it.
2. **There is no connection limit to hit.** A "connection" here is an in-process object over a
   bucket. The thing that actually kills Apartment — `max_connections`, PgBouncer, session mode —
   does not exist in this architecture.
3. **It parallelises freely.** No advisory locks, no shared `search_path`, no server-side
   coordination. The 5.2 minutes above is one thread being lazy.

And you don't have to pay even that, because:

## The design: lazy, per-tenant, idempotent

`tenant/migrate!` runs on **first touch**:

```clojure
(defn- open-tenant! [pool slug]
  ... (d/connect cfg)
      (schema/ensure-schema! conn)
      (migrate! conn slug))          ;; <- applies any pending norms, once
```

It calls Datahike's [`datahike.norm`][norm]: every EDN file under `resources/migrations/` is
transacted in filename order, and each is stamped with `:tx/norm` so it is **never applied twice**.

```clojure
;; resources/migrations/001-issue-severity.edn
{:tx-data [{:db/ident       :issue/severity
            :db/valueType   :db.type/long
            :db/cardinality :db.cardinality/one}]}
```

Deploy the file. Nothing else happens. Then:

- A tenant with traffic migrates on its next request — **31 ms**, inside a request that was going
  to open a connection anyway.
- An idle tenant migrates **when it wakes**, weeks later, or never.
- A **partial rollout is not a broken state.** Every database knows which norms it has. There is no
  "some are migrated and some aren't and I don't know which" — that's a property of centralised
  push, and it's the reason it hurts. Re-running converges; there is nothing to reconcile.

This is exactly where Turso landed. It is not a workaround; it is the shape the problem wants.

**What you give up:** for the duration of the rollout, the fleet is heterogeneous. Your app must
tolerate both schemas. That is **expand/contract**, and it is true of *any* zero-downtime migration
anywhere — you just usually get to pretend otherwise because the migration ran in a window.

The template's own test demonstrates the trap in miniature: after `001` adds `:issue/severity`,
`domain/issue`'s pull pattern still doesn't mention it, so the attribute exists and reads back as
`nil` through the app. **The schema migrates first; the code catches up after.** Write your code to
tolerate the gap, and only *contract* (remove the old shape) once every tenant has migrated — which
means you need to know that they have. See "what's missing" below.

## Rewriting migrations: expand, backfill, cut over, contract

Additive is easy. The hard case is a migration that **rewrites** data — a rename, a type change, a
restructuring. You cannot do that in place, here or anywhere.

The standard answer is an **online schema change**, the same shape as `gh-ost` /
`pt-online-schema-change`:

1. **Expand** — add the new shape alongside the old. Both exist; nothing breaks.
2. **Backfill** — transform old → new, in batches, while the tenant keeps serving.
3. **Dual-write** — new writes populate both shapes, so the new one stays current.
4. **Cut over** — switch reads to the new shape.
5. **Contract** — drop the old shape once nothing reads it.

Those tools spend nearly all of their complexity on **step 4**, because in a relational database
cutover means triggers, dual writes and a lock. Here it does not:

> **The branch head is the only mutable cell in the store.** A cutover is flipping one object —
> and it can be flipped with a **compare-and-swap** ([`If-Match` on S3](ha.md)). Build the new
> database (or branch) alongside, catch it up, then CAS the head. Any writer still holding the old
> head is *rejected*, not merged.

That is the same primitive as writer fencing, and it is not a coincidence: both problems are "make
exactly one version of the truth win, atomically." It falls straight out of the storage model.

A per-tenant variant, which is the natural fit for db-per-tenant: build the migrated database as a
**new store**, replay the tail of transactions that landed during the backfill, then swap the
tenant's slug → store-id mapping. `lifecycle/export-tenant` + `restore-tenant!` are the primitives
(`restore-tenant!` already rewrites refs, which is exactly what a restructuring migration needs);
the tail-replay and the swap are what you'd add.

## What's missing

Honest list — this template ships the lazy path, not the rest:

| piece | status |
|---|---|
| Lazy per-tenant migration on first touch | **shipped** (`tenant/migrate!`, tested) |
| Idempotent, resumable, per-database norm tracking | **shipped** (`datahike.norm`, `:tx/norm`) |
| Additive migrations | **shipped** — 31 ms/tenant |
| A **tenant registry with versions** — "have all tenants migrated, so I can contract?" | **missing**. Without it you cannot safely drop the old shape. This is the single most important gap, and it is small: a table of `slug → last-applied-norm`, updated by `migrate!`. |
| Backfill / dual-write / cutover tooling for rewriting migrations | **missing** — the design above, not built |
| Forcing a migration ahead of traffic (drain the tail before contracting) | **missing** — a background worker that walks the registry and touches idle tenants |
| Rollback | **missing**, and mostly the wrong idea: prefer forward-only norms. |

The gap is not the migration mechanism — that works. The gap is **knowing where you are**: a
registry that tells you which tenants are on which norm, so `contract` is a decision instead of a
hope.

[rails]: https://techvinta.com/blog/rails-multi-tenancy-row-vs-schema-vs-database
[turso-dep]: https://docs.turso.tech/features/multi-db-schemas
[turso-blog]: https://turso.tech/blog/how-to-deploy-schema-changes-to-a-million-databases
[norm]: https://github.com/replikativ/datahike/blob/main/doc/norms.md
