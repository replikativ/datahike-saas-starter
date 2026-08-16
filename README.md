# datahike-saas-starter

### Give every tenant their own database — and actually afford it.

Per-tenant databases are the isolation model everyone wants and few can pay for: a Postgres
per customer means a *provisioned server* per customer. So most SaaS shares one schema behind
a `tenant_id` column and hopes the `WHERE` clause is never wrong.

This template makes a tenant nearly free. On [Datahike](https://github.com/replikativ/datahike)
over object storage, a tenant is **a handful of objects in a bucket (~1 ¢/month)** and a
megabyte or so of RAM while it's open — so one small VM holds thousands of them, each a genuinely
isolated database you can query, export, clone, or **delete outright** (`datahike-saas.kernel.lifecycle`).
Offboarding a customer is deleting their database, and GDPR erasure is complete by construction:
their data lives in no other tenant's.

The demo is a small **issue tracker**, one Datahike database per tenant.

## Quickstart — 30 seconds, no Docker, no account

```bash
clj -M:dev            # nREPL on :7888. That's it.
```

```clojure
(go)                       ; open a tenant pool
(def i (:first-issue (seed! "acme")))   ; a couple of users, labels, issues
(open-issues "acme")       ; => issues, newest first
(def b (attach! "acme" i "README.md"))  ; a blob attachment — bytes in the tenant's store
(String. (fetch "acme" b)) ; ... the bytes back (see doc/blobs.md)
(seed! "globex")           ; a second tenant — a second DATABASE
(delete! "acme")           ; ... and it's gone. Not the rows — the database (blobs included).
```

No container to install, no cloud account, no credentials. Each tenant is a real Datahike
database in a folder under `data/`. When you want a real bucket, it's **one environment
variable** — the domain code, schema and queries never change. That is the whole argument, and
you just ran it.

> **Want to know how it works before you build on it?**
> **[doc/architecture.md](doc/architecture.md)** is the map — the module graph (a reusable
> *kernel* + a replaceable issue-tracker *example*), a request traced end-to-end, and the
> tier ladder — and **[doc/building-your-own.md](doc/building-your-own.md)** is the four files
> you write to put your own domain on the kernel.

### Or as an HTTP service

```bash
PORT=8899 clj -M:run
```

```bash
curl -XPOST localhost:8899/t/acme/issues -H content-type:application/json \
  -d '{"title":"Login rejects valid tokens","reporter":"alice","priority":"issue.priority/urgent","labels":["bug"]}'
curl localhost:8899/t/acme/issues            # list open issues
curl localhost:8899/t/acme/stats             # counts by state/priority
```

A tenant-scoped JSON API (`/t/:tenant/...`) — issues, comments, close, stats, search. The
tenant slug in the path selects its database; users and labels are upserted on first
reference, so there's no separate setup step.

## The payoff: a tenant is a database, so offboarding is a delete

That `(delete! "acme")` above is the whole argument. It is genuinely hard in a shared-schema SaaS
and ordinary here (`datahike-saas.kernel.lifecycle`):

```clojure
(export "acme")                      ; every datom — a self-contained takeout
(export-edn "acme")                  ; ... as EDN; readable without Datahike
(clone! "acme" "acme-staging")       ; restore == clone: a staging copy of a real customer
(delete! "acme")                     ; the database is gone. Not the rows — the database.
```

Over HTTP, on the writer:

```bash
curl -O -J localhost:8899/t/acme/export     # takeout: the tenant as EDN
curl -XDELETE localhost:8899/t/acme         # => {"deleted":"acme"}   (404 if unknown)
```

Read replicas refuse the `DELETE` with a 405 — deletion is the writer's job.

**Offboarding is one call.** No `DELETE FROM … WHERE tenant_id` across forty tables, no orphan
rows, no wondering whether you missed one. **GDPR erasure is complete by construction**: their
data lives in no other tenant's database — there is no second table to sweep and no `WHERE` clause
to get wrong. (At Tier 4, remember each read replica also holds a local LMDB *cache* of the nodes it
read; erasure there means sweeping or wiping those caches too. `delete-tenant!` refuses to run on a
replica, because deleting from a cache peer would delete the tenant out of the writer's bucket.)
And *clone* is
the same code path as *restore* — a staging copy of a real customer, or a support repro against
their actual data, without touching production.

**And "do I now run 10,000 migrations?"** — no: each tenant applies pending migrations the first
time it is opened after a deploy (`resources/migrations/`, ~31 ms/tenant, idempotent, resumable).
No window, no orchestrator, and a partial rollout isn't a broken state because every database knows
which norms it has. It's the objection that kills db-per-tenant elsewhere;
[doc/migrations.md](doc/migrations.md) answers it with numbers, and with the expand/backfill/
cut-over design for rewriting migrations — where the cutover is a single compare-and-swap on the
branch head.

`d/delete-database` and `d/datoms` are **stable** Datahike API — unlike the write-amplification
knobs above. (`deps.edn` pins the versions this template needs.) Covered end-to-end in
`test-integration/`, including that a clone
preserves the **graph** — refs, cardinality-many labels, component comments and enum refs all
survive. Entity ids are not stable across databases, so `restore-tenant!` rewrites every ref;
a restore that replays them verbatim keeps the scalars and silently drops the graph.

> ⚠️ **There is no authentication.** The tenant slug is read straight from the URL path, so
> anyone who knows a slug can read that tenant. Put your authn in front of `conn-fn`: resolve the
> caller's tenant from a verified token and ignore the path segment. What db-per-tenant buys you
> is *storage* isolation — a bug in your app cannot leak across tenants, because the slug selects
> a different **database**, not a `WHERE` clause — and that is not a substitute for authenticating
> the caller.

## Your first real bucket

One env var. We use [Tigris](https://www.tigrisdata.com/) — free tier, a signup you can finish in
a minute, and it is the **fastest store for small writes**, which is exactly what a commit is
([the sourced comparison](doc/cost-model.md#picking-a-store): R2 is slow on small PUTs; Hetzner's
bucket is HDD-backed and tuned for ≥1 MB files). Any S3-compatible store works.

```bash
export SAAS_TIER=tier2
export S3_ENDPOINT=https://t3.storage.dev
export S3_BUCKET=my-tenants AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
clj -M:run
```

Same app. Same code. Your tenants are now objects in a bucket — **about a cent each per month**
([cost model](doc/cost-model.md)).

## When you outgrow one node

The rungs exist so you don't have to rewrite anything when you hit a wall. **Read
[`doc/ladder.md`](doc/ladder.md)** for the design; the one-line version:

| | when | what changes |
|---|---|---|
| **Tier 2** — one node, real bucket | you have users | nothing but the bucket |
| **Tier 3** — stateless read replicas | reads exceed one node (~300 ops/s) | more processes, same store |
| **Tier 4** — streamed head + local LMDB | the read *tail* hurts, or the working set exceeds RAM | a kabel writer + tiered store |

Tiers 3 and 4 add *processes*, not a different database. The store stays object storage the whole
way — that's the point.

> **Experimental, on purpose.** The knobs that make a commit cost one PUT (root fusion, diff-buf,
> commit-graph opt-out) and the Tier-4 streaming stack are all marked *Experimental* in Datahike.
> They exist to make this use case work, and this repo is where they get exercised — benchmarked
> and regression-tested against every rung. Build on it with that in mind.

## What makes this economical

A tenant is only cheap if a small commit costs **about one PUT**. At a dozen PUTs per commit,
per-tenant object storage is a bad idea — request bill and write latency both scale with it.
Three store-fixed Datahike knobs (identical across every tier, see `config.edn`) get it to
**1.00**, measured (`bin/put-count`):

| knob | what it buys | what it costs |
|---|---|---|
| `:fuse-index-roots? true` | saves one PUT per index touched (2–6/commit) and a GET on cold open | a fused db record is bigger and shares less — minor, and only if you retain records |
| `:index-config {:diff-buf-size 256}` | **25× less stored-object growth** on deep workloads | CPU, not IO — a cold full-range scan projects buffered diffs. Create-time-fixed. Newest of the three |
| `:commit-graph? false` | drops the per-commit provenance object | **declines a feature** — no `branch!`-from-a-commit, ancestry, merges, audit chain |

None of them is free, and the third is a *feature trade* rather than a cost.
**[doc/cost-model.md](doc/cost-model.md)** has the full reasoning, the measurements, and the
dollars. Time travel (`as-of`/`history`) is a **separate** knob (`keep-history?`), unaffected by
any of this.

## Attachments — files, and the GC keeps them alive

Issues carry blob attachments (screenshots, crash dumps) via Datahike's new
`:db.type/store-ref` — a datom value that **names an object** the garbage collector then
**marks**. The rule is *the database is the root set*: an object lives while a datom names it,
so **`delete!` erases a tenant's attachments with the tenant** and GDPR erasure stays complete
by construction. The same type covers two deployments, and the repo implements both
(`example.attachments`):

- **in-store** — bytes in the tenant's own konserve store (`attach!` / `fetch`); `d/gc-storage`
  reclaims them, portably, on every backend. For moderate sizes.
- **presigned S3-direct** — the browser `PUT`s straight to a bucket prefix (bytes never touch
  your JVM); datahike hands you the live set (`reachable-store-refs`) and you sweep your prefix.
  The only shape that scales to large files.

```bash
B64=$(base64 -w0 screenshot.png)
curl -XPOST localhost:8899/t/acme/issues/$ISSUE_ID/attachments -H content-type:application/json \
  -d "{\"filename\":\"shot.png\",\"content-type\":\"image/png\",\"data\":\"$B64\"}"   # => {"blob-id":…}
```

Content-addressed (a `hasch` hash), so re-upload is idempotent, identical bytes share one
object, and an `as-of` read of an old reference yields the old bytes. **[doc/blobs.md](doc/blobs.md)**
has the full story — the write window, why the id is content and not a path, and what it is
*not* for.

## Beyond this template

**Build your own domain on the kernel:** keep `kernel/`, replace `example/`. The kernel gives
you the pool, tier config, lifecycle and HTTP server; you write four small files (schema,
domain, routes, a composition root). **[doc/building-your-own.md](doc/building-your-own.md)**
walks it end to end.

**Your app speaks SQL?** [**pg-datahike**](https://github.com/replikativ/pg-datahike) embeds a
PostgreSQL-compatible adapter — wire protocol, SQL translator, `pg_*` catalogs — inside a
Datahike process, so clients that speak Postgres (psql, pgjdbc, psycopg2, Rails, Hibernate,
Odoo, Flyway) talk to it with no Postgres install. Point it at the store profiles here and the
result is **db-per-tenant Postgres on object storage**, scaling horizontally the same way — you
keep your ORM and your migrations.

**No server at all?** [**datahike-serverless**](https://github.com/replikativ/datahike-serverless)
is the same store on ephemeral compute — AWS Lambda first, with Cloud Run and Fly as sibling
profiles. It is the opposite trade to the ladder here: a tenant that nobody touches costs
*storage only*, and you pay the cold start instead of an instance. Worth reading its
measurements before choosing — the crossover is real, and **small tenants like this
template's need none of it** (at 5 issues the fused db record *is* the database: 1 GET, zero
node reads, [§7](doc/benchmarks.md)). Cold start is a big-single-database problem.

**Your storage isn't S3?** The `:store` is just a [konserve](https://github.com/replikativ/konserve)
backend, and there are many: **GCS**, **JDBC**, DynamoDB, Redis, RocksDB, LevelDB, LMDB, the
filesystem, in-memory. Anything with similar economics slots into the same four-tier story, and
writing a new backend is a small protocol implementation — the tiered-store trick Tier 4 uses
(`{lmdb, s3}`) composes over any pair of them.

## Layout

The code splits in two — a reusable **kernel** and a replaceable **example**. Arrows only
point example → kernel; nothing in `kernel/` names anything in `example/`. **New here?
[doc/architecture.md](doc/architecture.md) is the map** (module graph, a request end-to-end,
the tier ladder). To build your own SaaS, [doc/building-your-own.md](doc/building-your-own.md)
is "delete `example/`, write four files."

```
resources/config.edn        every tier side by side — the scaling story as data
data/                       the :local tier — one folder per tenant (gitignored)
src/datahike_saas/
  kernel/                   REUSABLE substrate — you don't edit this to ship
    config.clj              load the base-cfg for a tier
    tenant.clj              bounded, lazy db-per-tenant connection pool (schema INJECTED)
    lifecycle.clj           export / delete / clone a tenant — the db-per-tenant payoff
    routes.clj              health + tenant-lifecycle routes (domain-agnostic)
    server.clj              Jetty + reitit + roles (writer/reader/streaming); DI seam
    streaming.clj           Tier 4 — kabel writer + tiered-store (lmdb+s3) read replicas
  example/                  the ISSUE TRACKER — replace this with your domain
    schema.clj              issue schema + `create-pool` (kernel pool + schema injected)
    domain.clj              tx fns + queries — TIER-AGNOSTIC (never changes across tiers)
    attachments.clj         blob attachments — :db.type/store-ref (see doc/blobs.md)
    routes.clj              tenant-scoped issue routes (the conn-fn seam)
    app.clj                 composition root + -main (clj -M:run) — wires domain onto kernel
dev/user.clj                REPL entry: (go) (seed! "acme") ...
bench/                      open-loop load tests + HdrHistogram percentiles  (see doc/benchmarks.md)
bin/                        put-count + reader-gets (real PUTs/GETs via MinIO trace),
                            latency-proxy (Tier-2 sim), tier4-demo, run-unittests
test/                       in-memory domain + config tests            (clj -M:test)
test-integration/           end-to-end against MinIO                   (clj -X:integration)
test-integration-tier4/     Tier-4 streaming guarantees against MinIO
                            (SAAS_TIER=tier4 LMDB_PATH=... clj -X:kabel:lmdb:integration-tier4)
doc/architecture.md         START HERE — the map: module graph, a request, the tier ladder
doc/building-your-own.md    keep the kernel, replace the example — the four files you write
doc/blobs.md                attachments via :db.type/store-ref — in-store + presigned S3
doc/ladder.md               the four-tier design, in depth
doc/benchmarks.md           reproducible load tests + Tier-1 results
doc/cost-model.md           $/tenant across providers; multi-tenant fleet back-of-envelope
doc/ha.md                   single-writer failover: fencing the head, and what's not built
doc/migrations.md           migrations across N databases — lazy, per-tenant, idempotent
resources/migrations/       EDN norms, applied to each tenant on first touch
doc/results/                raw benchmark output (edn)
docker-compose.yml          profiles: tier1 (MinIO) · tier3 (writer + direct reader)
                            · tier4 (kabel writer + streaming reader)
```

## Benchmarks

Reproducible load tests live in [`bench/`](bench/datahike_saas/) with results and analysis
in [`doc/benchmarks.md`](doc/benchmarks.md). Headlines from Tier 1 (local MinIO):

- **Write economy** — the default config commits in **1 PUT** (branch head, in place). *Fusion*
  is the lever: it saves one PUT per index touched (2–6/commit; 4 → 1 here), and those PUTs are
  sequential, so they are latency too. *diff-buf* doesn't move the PUT floor — it keeps
  stored-object growth ~25× lower on deeper workloads (storage + GC), at the cost of CPU, not IO.
  On object storage, where each PUT is cost *and* latency, this is the difference between viable
  and not for per-tenant databases. (`bin/put-count`, `doc/benchmarks.md` §1.)
- **Read-heavy load** — 200 ops/s sustained on one node, reads **p50 ~2 ms / p99 ~31 ms**,
  writes **p50 ~31 ms / p99 ~66 ms** (open-loop, coordinated-omission-aware). One node
  saturates around **~300 ops/s**.
- **Density** — scales with tenant *data*, so quote it with the tenant size: **0.09 MB/tenant**
  at 5 issues, **1.30 MB/tenant** at 100. There is no per-tenant server and no pooled connection
  to pay for — that's the whole argument. The pool is **bounded** (`SAAS_MAX_HOT`, default 512),
  so heap tracks the *concurrently active* set, not the fleet; an evicted tenant pays ~+52 ms on
  its next request ([§3b](doc/benchmarks.md)). Throughput bites too: one node saturates near
  **~300 ops/s**.
- **Cold start** — a short-lived reader (a lambda) pays *round trips*, not request price: wall
  time is `misses x RTT` with **zero** overlap (measured **24.96 ms** per extra node against a
  ~25 ms round trip). Fetching the same key set at 64-way concurrency is **16.4x** faster, and a
  budget-bounded BFS warm reaches "the query costs 0 GETs" in **37** fetches where the tiered
  store's built-in preload needs **392**. Small tenants need none of it — at 5 issues the fused
  db record *is* the database, 1 GET. The cold-start problem is a *big-single-database* problem,
  and db-per-tenant dissolves it. ([§7](doc/benchmarks.md))
- **Cost** — object-store ops run **~1 ¢/tenant·month**; the bill is compute plus, on AWS,
  *egress to your users* — which outweighs the bucket. See [doc/cost-model.md](doc/cost-model.md).
- **GC** — with `:commit-graph? false` no commit record pins an old index root, so essentially
  all superseded state is reclaimable (**99.7%** in the churn test); diff-buf keeps steady-state
  garbage low to begin with. (`keep-history?` doesn't affect this — history lives *inside* the
  temporal indices as live data, not as garbage.)

```bash
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload compare   # net object growth (storage/GC)
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload mixed     # open-loop read/write
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload scale     # tenants/node density
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload gc        # storage reclamation
bin/put-count                                                    # real PUTs/commit (trace)
bin/latency-proxy 40                                             # Tier-2 sim: +40ms RTT
```

## Status

Every rung is implemented and exercised; see [`doc/ladder.md`](doc/ladder.md) for the design.

| tier | state |
|------|-------|
| **local (files)** — the default | Working, and the zero-setup path: no Docker, no account, no credentials. A real Datahike database per tenant in a folder. Does *not* exercise the object-store write path — for that, use tier1 or a real bucket. |
| **1 — MinIO** | Working. The real S3 write path, locally. This is what the benchmarks, the trace-based counters and CI run against. |
| **2 — real bucket** | Config profile ready and integration-tested against MinIO. **Not yet run against a real cloud bucket** — Tier-2 latency here comes from a proxy (`bin/latency-proxy`), so the true tail is unverified. |
| **3 — direct readers** | Working. A stateless reader derefs the shared bucket and auto-follows the writer — no streaming, no sync. Costs **1 head GET per deref** (measured). Readers *must* run `SAAS_ROLE=reader`: the follow works because that sets a non-streaming writer backend, and a node left on the default `:self` writer serves a **frozen snapshot** silently. |
| **4 — streaming + LMDB** | Working. Kabel writer streams commits to readers on a tiered `{lmdb, bucket}` store; reads hit **p50 0.08 ms with no S3 round trip**. Guarded by `test-integration-tier4/`, because a warming regression is otherwise *silent* — queries still pass, they just quietly round-trip to S3. |

Tiers 3 and 4 lean on Datahike's streaming stack (kabel writer, konserve-sync, tiered stores),
which is likewise **experimental** and moving. If you need Tier 4 today, expect to track
upstream releases; Tiers 1–2 are the conservative choice.

## License

MIT — see [LICENSE](LICENSE). This is a template: fork it, gut it, ship it.
