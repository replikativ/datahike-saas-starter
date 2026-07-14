# The scaling ladder

Four rungs, each introduced when the previous one's limit is hit. The guiding property:
the **application never changes** — `datahike-saas.domain` takes a connection and runs
transactions and queries, oblivious to what's underneath. What changes is the *store* and,
for the read-scaling rungs, the *topology*.

Honesty up front: **Tiers 1→2 are a pure config swap. Tiers 3→4 add processes.** The
storage substrate stays object storage the whole way; scaling reads means running more
copies of the reader, not a different database. That distinction is called out at each rung.

---

## Local — a folder (the default; not really a rung)

**What:** `:file` — one directory per tenant. No Docker, no account, no credentials.
**Why:** see the model work in thirty seconds. It is a real Datahike database per tenant, and the
domain code, schema and queries are identical to every rung below.
**Store:** `:file` at `data/tenants/<store-id>/` (a konserve file store *is* a directory, so each
tenant needs its own).
**What it does NOT do:** exercise the object-store write path. PUT counts and RTT are meaningless
here — that is what Tier 1 is for.
**Limit that pushes you up:** it's a folder on one machine.

## Tier 1 — local dev against the real object-store path (MinIO)

**What:** MinIO on your laptop speaking S3; one process is both writer and reader.
**Why:** exercise the *actual* konserve-s3 write/read path — PUTs, GETs, round trips — with zero
cloud setup. This is what the benchmarks, the trace-based counters (`bin/put-count`,
`bin/reader-gets`) and CI run against, and it is the reason MinIO is still here.
**Store:** `:s3` with an `:endpoint-override` at `localhost:9000`.
**Limit that pushes you up:** it's your laptop — not durable, not shared.

## Tier 2 — real object storage (Tigris / AWS S3)

**What:** the same `:s3` backend pointed at a real bucket. A single always-on server holds
the writer and serves reads.
**Why:** durable, cheap, and genuinely multi-tenant — thousands of per-tenant databases in
one bucket, each a handful of objects.
**Store:** `:s3`; only creds + `:s3-endpoint` change (unset ⇒ plain AWS). **This is the
pure config swap** — literally the same code, one env var different from Tier 1.

Three things to get right here:

- **Pick a store that is fast at *small* writes.** A commit is a few-KB object, which is the
  workload object stores differ on most. Tigris and S3 land in the tens of ms; Cloudflare R2 is
  slow on small PUTs; Hetzner's bucket is HDD-backed and tuned for ≥1 MB files, so its free
  operations don't help here. Sourced comparison in [cost-model.md](cost-model.md).

- **The writer wants to be a persistent singleton, not a lambda.** It batches transactions
  and holds the in-memory db + write buffer; a lambda cold-starts and must re-attach to the
  head each invocation (higher write latency), and two concurrent invocations would violate
  the single-writer model. Reads *are* stateless and can scale out (that's Tier 3) — but the
  write side wants one small always-on instance (or provisioned-concurrency = 1). Native-image
  (GraalVM) trims cold-start if you do go serverless, at the cost of reflection config across
  the konserve/fressian/PSS stack.
- **The single writer is a write SPOF**, and the failure to prevent is not "the writer is down"
  — it's **two writers thinking they're the writer**, which silently loses updates. The fix is to
  make the object store refuse the stale one: condition the branch-head write on the ETag the
  writer read, so a fenced writer gets a 412 instead of clobbering. `:commit-graph? false` is what
  makes that work — the commit is a *single* object write, so fencing that one write fences the
  whole commit. The primitive exists and is verified; the Datahike side is not built.
  **[doc/ha.md](ha.md)** has the design, the measurements, and an honest list of what's missing.
  **Reads stay up regardless** — they only need the bucket.

**Limit that pushes you up:** read throughput (or tail latency) exceeds one box.

## Tier 3 — scale reads (direct S3 readers, no streaming)

The shared S3 store is the single source of truth. Scaling reads is just **running more
reader nodes against it**, and a reader is *stateless*: it connects to the shared S3 and just
derefs. A **non-streaming** connection re-reads the branch head from the store on every `@conn`
deref, so the reader **auto-follows the writer with no streaming and no sync** (`SAAS_ROLE=reader`;
`reader-demo`). Add readers freely behind a load balancer; each knows only the bucket.

**Measured cost: exactly 1 head GET per deref** (`bin/reader-gets`, MinIO trace), plus cold nodes
on a cache miss. That is the honest trade — Tier 3 scales read *throughput*, but every read pays
one object-store round trip (~14 ms on local MinIO, your bucket's GET RTT in production). Removing
that round trip is what Tier 4 is for.

> ⚠️ **The trigger is the writer backend, not the absence of one.** `deref-conn` re-reads the head
> only when the writer is **non-streaming**. The default `:self` writer is `:streaming? true`, so
> `@conn` on it returns an in-memory atom. A reader follows the writer *because* it configures a
> `:datahike-server` writer. **A node left on the default `:self` writer serves a frozen snapshot
> forever — silently.** So a Tier-3 reader must set `SAAS_ROLE=reader` (which does this for you);
> "just run more tier3 nodes" would give you N `:self` writers on one bucket, each frozen and each
> claiming write authority. This is the sharpest edge in the whole ladder.

**No LMDB here** — the branch head is a *mutable* key, and caching it locally would serve
stale reads. The immutable-node cache that LMDB provides is a Tier-4 concern, where the head
arrives from a stream instead of being re-read from the store.

**Read and write are independent config axes**, decoupled from the writer *instance*:

| axis | config key | Tier-3 choice |
|------|-----------|---------------|
| **read** (`:store`) | shared S3 (direct) | `@conn` re-reads the head each deref |
| **write** (`:writer`) | `:self` (writer node, *streaming*) · `:datahike-server` (HTTP → writer, *non-streaming*) | a reader MUST use `:datahike-server` — that is what makes its reads follow |

- **Request partitioning** (sticky hash on the tenant slug at the LB) is a cache-locality
  optimization — it raises a node's result/node-cache hit rate; skip it and it just runs colder.

**Limit that pushes you up:** you need lower staleness, or a reader's working set exceeds RAM
so cold node reads fall through to S3 and p99 spikes — both are Tier 4.

## Tier 4 — low staleness + bounded tail (streaming + lmdb+s3)

**What:** a single kabel writer **streams** each commit to readers whose store is **tiered
`{lmdb, shared-s3}`**.
**Why two things at once:** the stream delivers the *head* in-memory (so `@conn` is fresh at
zero round-trips — lower staleness than Tier 3's per-deref head read), and the tiered store
serves *node* reads from local **LMDB** (sub-millisecond) before falling back to the shared S3
(bounded tail when the working set exceeds RAM). They belong together: because the head comes
from the stream, the LMDB only ever caches **immutable** nodes — so there is no stale-head
problem that a direct reader would hit.
**Store:** reader = `:tiered {:frontend-config {:backend :lmdb} :backend-config {:backend :s3}}`
with `:write-policy :frontend-only` — the peer caches locally and **never writes the shared,
writer-owned S3**. (The keys really are `:frontend-config`/`:backend-config`; plain `:frontend`/
`:backend` would collide with `:backend :tiered` itself.) The writer holds the S3 backend directly
(a local cache belongs on the readers, not the single writer). `SAAS_ROLE=reader-streaming`;
`tier4-streaming-demo` reads at **p50 0.08 ms, no S3**.

**Two channels, by mutability** — this is the whole design in one table:

| | delivered by | why |
|---|---|---|
| **the branch head** (mutable) | the writer's kabel stream | must be fresh; it's tiny; latency-critical |
| **index nodes** (immutable) | the local LMDB, falling back to shared S3 | bulk; content-addressed, so never stale; S3 scales horizontally, the single writer doesn't |

Because the head arrives from the stream, the LMDB only ever caches *immutable* nodes — so the
cache can't go stale, and a direct reader's stale-head problem simply doesn't exist here.
konserve-sync also **pushes each commit's nodes into the reader's LMDB** as they're written, so
the cache stays warm without polling.

**Connect is not free.** On connect, konserve-sync runs datahike's reachability walk and pushes
every key the reader lacks — and for a `:frontend-only` reader the subscribed store is the *empty*
LMDB frontend, so on a fresh reader that means **every reachable node**. The connection also
**blocks until that push drains**, as it must: exposing a head whose nodes haven't landed is how a
query walks into an absent node. So each reader join costs a full-tree walk plus a bulk push from
the single writer, ×N readers — plan for the burst. (Bulk warming would sit better on the reader's
own S3-pull path: writer-independent, parallel, and S3 scales where the single writer doesn't.)

- **Watch sync fan-out.** Pushing to N replicas fans deltas ×N from the writer; at large N,
  put a pub/sub or a hierarchical sync hub between writer and replicas rather than fanning
  out directly. Each replica's LMDB is a near-full local replica (disk cost) — the right
  trade only in the big-working-set / tight-p99 regime, the wrong reach earlier.
- **GC is split by tier.** The **writer** GCs the shared S3 backend (`d/gc-storage`). Each
  **reader** reclaims its own LMDB with `konserve.gc/sweep!` on the *frontend store*, against the
  writer's reachable set — the backend is untouched. `tier4-gc-demo` runs a real streaming
  replica and reclaims **96%** of its LMDB cache with the shared S3 unchanged and the replica
  still current ([benchmarks §6](benchmarks.md)). (Fallback: wipe + re-warm.) Sweeping the
  frontend directly is
  the clearest way to say what you mean — though note that under `:write-policy :frontend-only`,
  konserve's tiered deletes are frontend-only anyway, so `d/gc-storage` on *this* store would not
  in fact reach S3. It would under `:write-through`/`:write-behind`/`:write-around`.

---

## Why the store is always "just config"

Everything under Datahike is a [konserve](https://github.com/replikativ/konserve) store behind
one storage protocol. `:s3`, `:tiered{:lmdb,:s3}`, memory — same interface, swapped by data.
That's why Tiers 1, 2, and 4 are config edits and Tier 3 adds only *processes*, never a port of
the app. The ladder is a demonstration of that property as much as a deployment guide.

The consequence is that **object storage is a choice, not a requirement**. konserve also ships
GCS, JDBC, DynamoDB, Redis, RocksDB, LevelDB, LMDB, filesystem and in-memory backends; any of
them drops into the same `:store` slot, and a new one is a small protocol implementation. The
tiered store composes over *any* pair — `{lmdb, s3}` here is one instance of a general
local-cache-over-shared-truth pattern.

## Benchmarks (per rung)

Each rung is measured against the limit it exists to solve — all open-loop, since a closed-loop
runner throttles itself and hides the tail. [**benchmarks.md**](benchmarks.md) has the numbers
and the commands:

| rung | the question it has to answer | where |
|---|---|---|
| **1→2** | does a commit cost one PUT? what does a real RTT do to it? | §1, §4 |
| **2** | how many tenants and ops/s does one node hold? | §2, §3b |
| **3** | do reads leave the object store entirely? | §5 |
| **4** | is the cold-read tail bounded when the working set exceeds RAM? | §6 |

Everything runs against the active `SAAS_TIER`, so the same harness that measures MinIO
measures your real bucket.
