# Benchmarks

Reproducible load tests for the starter. All run against the active tier
(`SAAS_TIER`), so the same harness measures Tier 1 (local MinIO) and, unchanged,
Tier 2 (real object storage) and beyond.

```bash
docker compose --profile tier1 up -d
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload compare   # write amplification
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload mixed     # open-loop read/write
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload gc        # storage reclamation
```

The harness (`bench/datahike_saas/harness.clj`) uses HdrHistogram and an
**open-loop** runner: ops fire on a fixed schedule and latency is measured from the
*intended* send time, so queueing under load is counted, not omitted (a closed-loop
runner throttles itself and hides the tail).

---

## 1. Store config — write amplification

Why it matters: on object storage every commit is one or more PUTs, and PUTs are the
dominant cost *and* latency. Per-tenant databases are only economical if a small
commit costs ~one object write. The comparison runs 300 mixed write commits
(create/comment/assign/close) against a fresh tenant per config and counts the konserve
objects produced.

There are **two distinct metrics** here, and they answer different questions:

- **PUTs/commit** — actual object-store write operations per commit. Drives **cost ($/PUT)
  and latency**. Measured out-of-band by counting `PutObject` in MinIO's request trace
  (`bin/put-count`), because the in-process view can't see it: every commit overwrites the
  branch head at a *fixed* key — a real PUT that adds no new key.
- **Net stored-object growth** — how much the persisted key-set grows per commit. Drives
  **storage size and GC pressure**. Measured in-process by `workload compare` (key-set
  delta over N commits). Can be < 1/commit precisely because of those fixed-key overwrites.

### PUTs/commit — cost & latency (`bin/put-count`, MinIO trace)

Marginal PUTs/commit `(PUT@40 − PUT@20)/20`, single-issue commits:

| config | PUTs/commit | what changed |
|--------|:-----------:|--------------|
| **economical** (fusion, no commit-graph) | **1.00** | one PUT — the branch head, in place |
| `no-diff-buf` (diff-buf 0) | 1.00 | same floor for shallow commits (see note) |
| `with-commit-graph` | 2.00 | + 1 provenance object per commit |
| `no-fusion` | 4.00 | + 3 separate index-root PUTs (eav/aev/avet) |

- **Fusion is the per-commit PUT lever.** An index's root is rewritten on essentially every
  commit, so inlining it into the db-record removes that PUT. The saving is **one PUT per index
  touched — 2 to 6 per commit** (eavt/aevt/avet, plus their temporal twins when `:keep-history?`
  is on). This tenant is history-off and touches all three primary indices, so it is 4 PUTs → 1.
  And because roots are written in *sequential* waves (a child must be durable before its parent
  references it), those PUTs are also latency: **75 → 119 ms** through the +40 ms proxy (§4).
  **commit-graph** off saves one more PUT.
  *What fusion costs:* the db record grows and shares less — without fusion, two commits that
  didn't change an index both point at the **same** root object; with fusion the root's bytes are
  copied into each record. That only matters if you **retain** records (`:commit-graph?` on), and
  it is minor. (Under `:crypto-hash?` fusion saves the cold-open GET but not the PUT.)
- **diff-buf does *not* lower the PUT floor for shallow commits** (1.00 either way here). Its win
  is *storage growth* as trees deepen — the next metric, not this one. **Its cost is CPU, not
  IO:** buffered diffs are projected onto nodes as they load, so a cold full-range scan does ~2×
  the node work; point lookups, counts and warm scans are unaffected, and there are **no extra
  object-store requests**. It is also the newest of the three knobs — promising, but wanting more
  testing before it's called stable.

### Net stored-object growth — storage & GC (`workload compare`, 300 mixed commits)

| config | growth/commit |
|--------|--------------:|
| **economical** (diff-buf + fusion) | **0.20** |
| `no-diff-buf` | 5.05 (**25× more**) |
| `no-fusion` | 3.21 |
| `with-commit-graph` | 1.20 |
| `full-history` | 1.45 |

Here **diff-buf is the big lever**: on the deeper 300-commit mixed workload it buffers
content-only child diffs into the rewritten ancestor, so far fewer new content-addressed
nodes persist — 25× less object growth than flushing every tree level. Less growth means
less storage *and* less to garbage-collect (§3). `no-fusion` and `with-commit-graph` each add
their own new keys per commit.

Read `full-history` carefully: that variant turns on **both** `:commit-graph?` *and*
`:keep-history?`, so its 1.45 is not the price of history. Against `with-commit-graph`'s 1.20,
history's own marginal cost here is ~**0.25 objects/commit** — the temporal-index nodes. Those
are **live data, not garbage**: they're what `as-of`/`history` read, and GC marks them reachable
from the head (§3).

> Net-growth being ~0 for economical is the ideal: the branch head overwrites **in place**,
> so storage stays flat and there's essentially no content-addressed garbage to collect.
> Write *latency* on local MinIO is round-trip-dominated; §4 puts realistic RTT in front of
> it, where the PUT count above becomes latency.

## 2. Mixed read/write — open-loop

A read-heavy SaaS workload (90% reads) spread across 20 tenants, fired open-loop at a
fixed target rate. Reads are the five domain queries; writes are the create/comment/
assign/close mix.

**Tier 1, MinIO, 20 tenants, 200 ops/s target, 90% reads, 20 s:**

| op | count | p50 | p90 | p99 | p99.9 |
|----|------:|----:|----:|----:|------:|
| read | 3619 | 1.97 ms | 5.13 ms | 30.56 ms | 54.39 ms |
| write | 381 | 31.05 ms | 46.07 ms | 65.99 ms | 84.67 ms |

Achieved **199.7 ops/s** (kept up with target — not saturated). Reads are served from the
in-memory index at p50 ~2 ms, with a tail when a cold node is fetched from the object
store; writes are one commit (1 PUT, §1) at p50 ~31 ms.

### What "multi-tenant" means here, and at scale

The workload is genuinely concurrent across tenants: **each tenant is a separate database
with its own hot connection**, and the `workers` threads route each op to a tenant (uniform,
or Zipf-skewed with `:skew` — a hot set + idle tail, like real traffic). Reads come from the
target tenant's in-memory cache; **writes to the same tenant serialize through its single
writer, but writes to different tenants run fully in parallel.**

Scaled up — **200 tenants, Zipf skew, on one node:**

| target | achieved | read p50 | read p99 | write p50 | write p99 |
|-------:|---------:|---------:|---------:|----------:|----------:|
| 120 ops/s | 119.5 | 2.9 ms | 35.7 ms | 36.5 ms | 128.7 ms |
| 300 ops/s | 292.9 | **376 ms** | 1431 ms | 714 ms | 1261 ms |

At 120 ops/s the node serves 200 active tenants comfortably (hot tenants at read p50 ~3 ms, a
modest cold tail). At 300 ops/s it **saturates** — the open-loop runner's latencies blow up
as the queue backs up (this is the runner working: it counts queueing, it doesn't hide it).
So a single node's ceiling here is a few hundred ops/s against a working set larger than
cache. Scaling read *throughput* past that is Tier 3 (more stateless readers on the shared
bucket); bounding the cold-read *tail* is Tier 4 (streamed head + LMDB). Run it:
`... workload mixed '{:tenants 200 :skew 1.0 :target-rate 120}'`.

### End-to-end HTTP

The same workload against the running HTTP service (`clj -M:run`), measured with a
separate client over the network — so latency includes Jetty, JSON, and routing:

| op | count | p50 | p90 | p99 | p99.9 |
|----|------:|----:|----:|----:|------:|
| read | 3612 | 3.52 ms | 21.50 ms | 296.22 ms | 558.89 ms |
| write | 388 | 48.01 ms | 85.59 ms | 339.74 ms | 345.24 ms |

The web layer adds only ~1.5 ms to the read median (2 → 3.5 ms). The **tail**, however,
inflates sharply (read p99 31 → 296 ms): it is dominated by storage/commit latency and
occasional pauses, and the open-loop runner counts the queueing those spikes cause. This is
precisely the tail that **Tier 4** exists to flatten (the head arrives on a stream, and a cold
node comes from local LMDB instead of S3) — the single-node Tier 1/2 story is "great median,
storage-bound tail." Note Tier 3 does *not* help this: a direct-S3 reader adds a head GET to
every read (§5); it buys throughput, not latency.

Run it yourself:

```bash
SAAS_TIER=tier1 PORT=8899 clj -M:run &                  # start the service
clj -M:bench -m datahike-saas.http-load '{:base "http://localhost:8899"}'
```

## 3. Storage reclamation — GC

Content-addressed commits leave superseded index nodes behind; `d/gc-storage` reclaims
everything the reachability walk doesn't reach. This run churns 800 updates over 50 issues
under `:no-diff-buf` (to *generate* garbage — see the note):

**Tier 1, MinIO:**

| | objects |
|--|--:|
| after churn | 2440 |
| after GC | 7 |
| **reclaimed** | **2433 (99.7%)** |

GC took **14.7 s** — that's 2433 object DELETEs plus the reachability walk against MinIO.

**What makes that 99.7% possible is `:commit-graph? false` — not `keep-history? false`.**
The walk starts at the branch head and follows each commit record's `:datahike/parents`. A
commit record holds *that commit's* index roots, so while it stays reachable, its entire tree
stays alive. With the commit graph off there are no parent records at all: the lineage ends at
the head, and every superseded root is unreachable the moment the head moves. (With the commit
graph *on*, you reclaim the same nodes by pruning the chain — `gc-storage`'s `remove-before`,
or background GC's `:history-window-ms`.)

**`keep-history?` is a live-data knob, not a garbage knob.** History lives *inside* the
temporal indices, and those are marked reachable from the current head — they are live data,
not collectable garbage. Turning history on doesn't pin more garbage; it means you write and
keep three more index trees. Turning it off doesn't reclaim more; it means those nodes were
never written. It has **no effect on what GC can reclaim.**

Two lessons, then: **(1)** what you can reclaim is governed by the commit graph and its retention
window — so a churn-heavy tenant either drops the graph or prunes it; and **(2)** GC is real work
(14.7 s here — 2433 DELETEs plus the walk), so you schedule it rather than run it inline.

It does **not** stall the writer, though. `gc-storage!` is safe to run *concurrently* with an active
writer. Marking is read-only, and the sweep stops at the store's **safe point** — the instant before
which every written object is either reachable or garbage. A commit writes every value the new head
references and only *then* flips the head, so for the duration of that sequence its objects are on
disk and named by nothing; the safe point is what tells the collector to leave them alone. Datahike
ships `datahike.gc/start-background-gc!` (periodic concurrent mark-and-sweep, multi-branch,
*Experimental*), so reclamation is a scheduled background job, not a stop-the-world pause.

> **All writers for a database run in one JVM** — and writer-side maintenance, including GC, runs with
> them. `d/gc-storage` is a writer operation, so it is already in the right place; the note is here
> because "collect from a cron job during the quiet hours" is a tempting shape and sits outside the
> model. Readers are unconstrained — that is the whole point of Tiers 3 and 4. This template keeps one
> writer per tenant in the app process, so there is nothing to arrange.

Keeping the graph is not the expensive choice it might look like. Datahike shares structure, so a
retained commit holds only the nodes that actually diverged from the current tree, not a copy of
the database — close to optimal, as ways of keeping history go. It buys branching, ancestry and
the audit chain; you drop it when you don't use them. See
[cost-model.md](cost-model.md#commit-graph-is-a-feature-youre-declining-not-just-a-cost).

Note the interaction with §1: **diff-buf both reduces garbage generation and reclamation
pressure.** The same churn under the `economical` (diff-buf) config produces only a few
dozen objects — most updates never materialize as separate objects, so there is little to
collect. GC matters most for high-churn tenants.

## 3b. Scale — many tenants on one node

`clj -M:bench -m datahike-saas.workload scale '{:tenants N :issues-per M}'` opens N tenant
databases (each its own Datahike db) and keeps every connection hot.

**Density is not a constant — it scales with tenant DATA.** A hot connection holds that
tenant's in-memory index, so the per-tenant heap is a function of what's *in* the tenant:

| tenant size | heap/tenant | objects/tenant | run |
|---|---:|---:|---|
| **5 issues** (a toy tenant) | **0.09 MB** | 8 | 1,000 tenants |
| **100 issues** (a modest one) | **1.30 MB** | 82 | 50 tenants |

Roughly linear in data, and **14× apart**. Earlier versions of this page quoted only the
0.09 MB figure — measured on 5-issue tenants — and then used it to size fleets of realistic
ones. Don't. At 1.30 MB, 10,000 hot tenants is **~13 GB of heap**, not 0.9 GB.

**So the pool is bounded.** It keeps at most `SAAS_MAX_HOT` connections open (default **512**)
and evicts the least-recently-used beyond that, so heap tracks the **concurrently active** set —
a hot head and a long idle tail, which is what real traffic looks like — instead of every tenant
ever touched. `:max-hot nil` restores unbounded behaviour (this benchmark uses it, to measure
MB per *hot* tenant).

**Eviction is not free, and here is the bill** (100-issue tenants, MinIO):

| read | p50 | p99 |
|---|--:|--:|
| connection hot | 8.7 ms | 45 ms |
| **after eviction** (reopen + fault the index back in) | **60.8 ms** | 101 ms |

**~+52 ms on the first request to an evicted tenant.** Size `SAAS_MAX_HOT` so that your *active*
set stays hot and evictions land on the idle tail. It is a reconnect, not a cold create: the
158 ms first-connect below is `create-database` + schema install, which an eviction never redoes.

Eviction is also **safe under load**, which is the part that takes care: a connection is never
closed while a request is reading it. Each HTTP request *pins* its tenant (`core/wrap-tenant-pin`)
and eviction skips pinned tenants — if everything is pinned, the pool exceeds `:max-hot` rather
than break a live request. A time-based reaper would be simpler and would occasionally close a
slow query's connection underneath it.

What density *does* buy, and it's the whole argument: there is no per-tenant server, no
provisioned database, no connection pool to pay for. A tenant costs a hashmap entry and its own
index. That is what makes db-per-tenant affordable at all — just don't read a toy tenant's
0.09 MB as a fleet-sizing constant.

**Throughput bites too.** One node saturates around **~300 ops/s** (§2), which a few thousand
*active* tenants reach regardless of RAM. Both ceilings are real; which one you hit first
depends on how fat and how busy your tenants are.

Cold first-connect is dominated by `create-database` + schema install (several sequential round
trips); a warm reconnect is far cheaper, and the tenant registry keeps connections hot after
first use.

## 4. Tier 2 (simulated) — object-store round-trip latency

Local MinIO measures your disk, not a network. To get a Tier-2-shaped picture without a
cloud bucket, put [toxiproxy](https://github.com/Shopify/toxiproxy) in front of MinIO and
inject a fixed downstream latency approximating a same-region cloud PUT/GET leg:

```bash
bin/latency-proxy 40                     # +40ms ≈ AWS S3 same-region (see calibration below)
MINIO_PORT=19000 SAAS_TIER=tier1 clj -M:bench -m datahike-saas.workload compare '{:commits 80}'
```

**Faithful for:** per-request RTT — konserve does discrete request→response, so each PUT/GET
pays a realistic delay, which is exactly what turns the write path's round-trip *count* into
latency. **Not modeled:** S3's throttling tail (`503 SlowDown`), bandwidth caps, TLS on new
connections, real concurrency scaling. So read the median as representative and the tail as
indicative; a real cloud run is still the authority on p99.

**Calibrating the proxy.** Published same-region small-object PUT latencies, to pick a number:

| store | small-object PUT | source |
|---|---|---|
| S3 Express One Zone | single-digit ms | [AWS][s3x] |
| **Tigris** | write tail <17 ms | [Tigris benchmark][tb] |
| **AWS S3 Standard** | median "tens of ms"; p90 1 KB PUT <38 ms | [AWS][s3perf], [Tigris benchmark][tb] |
| **Cloudflare R2** | p90 1 KB PUT >340 ms — avoid for writes | [Tigris benchmark][tb] |
| **Hetzner Object Storage** | HDD-backed; "not designed for ... single-digit millisecond response times", optimized for ≥1 MB files | [Hetzner][hz] |

The cross-provider figures come from Tigris's own competitive benchmark — a vendor measuring its
rivals — so weigh them accordingly; the AWS median and the Hetzner guidance are each from the
provider's own docs. **Better: measure yours.** `SAAS_TIER=tier2` with real credentials points
every benchmark in this file at your actual bucket — that's what the harness is for.

[s3x]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-high-performance.html
[s3perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html
[tb]: https://www.tigrisdata.com/blog/benchmark-small-objects/
[hz]: https://docs.hetzner.com/storage/object-storage/faq/general/

### Write config, through +40 ms (≈ AWS same-region), 80 commits

| config | objects/commit | write p50 | write mean |
|--------|---------------:|----------:|-----------:|
| economical | 0.05 | **74.97 ms** | 76.97 ms |
| no-diff-buf | 1.16 | 76.22 ms | 91.70 ms |
| no-fusion | 3.05 | **118.95 ms** | 119.35 ms |
| with-commit-graph | 1.05 | 68.16 ms | 71.10 ms |

The proxy **separates object count from latency**, which the local run can't:

- **Fusion is the clearest per-commit latency win.** `no-fusion` adds ~44 ms — one full RTT
  — to *every* commit's p50 (75 → 119 ms), because the index roots can't be inlined into the
  db record and go out as a separate, sequential PUT wave. A commit's latency tracks the
  number of **sequential** PUT waves (a child must be durable before its parent references
  it), and fusion removes one wave.
- **diff-buf's win is mean/tail and cost, not p50.** At 0.05 vs 1.16 objects/commit the
  object count differs ~20×, but p50 is similar (~75 ms) because when nodes *do* flush they go
  out concurrently within a wave. The payoff is the lower **mean** (77 vs 92 ms), the far
  smaller object count (→ $/PUT and GC pressure, §1/§3), and fewer heavy-flush tail spikes.

So on real object storage: **fusion buys a round trip on every commit; diff-buf buys cost and
tail.** Both are on by default.

## 5. Tier 3 — direct-S3 read replicas

A Tier-3 reader is **stateless**: it connects to the shared bucket and derefs. No writer
connection for reads, no streaming, no sync. It stays current because a **non-streaming**
`@conn` re-reads the branch head from the store on every deref — so it always sees the
writer's latest commit. Scaling reads is just running more readers behind a load balancer.

**The mechanism has a sharp edge worth knowing.** `deref-conn` re-reads the head *only when
the writer backend is non-streaming*. The default `:self` writer reports `:streaming? true`,
so `@conn` on it returns an in-memory atom. A Tier-3 reader follows the writer **because** it
configures a `:datahike-server` writer — that is the trigger, not the absence of one. Leave a
reader on the default `:self` writer and it will silently serve a **frozen snapshot forever**
(see [ladder.md](ladder.md)). `SAAS_ROLE=reader` sets this for you.

```bash
SAAS_TIER=tier1 clj -M:bench -m datahike-saas.reader-demo   # correctness + latency
bin/reader-gets                                             # real GETs/deref, from the trace
```

**Tier 1, MinIO, 20-issue tenant, 200 reads:**

| | p50 | p99 |
|--|--:|--:|
| **reader** (direct S3 — head GET per deref) | **14.0 ms** | 22.6 ms |
| writer (local `:self` conn, for reference) | 0.03 ms | 0.14 ms |

**Cost: 1.00 GET per deref** — measured from MinIO's request trace (`bin/reader-gets`), not
from timings. So a Tier-3 read is *one object-store round trip*, plus cold nodes on a cache
miss. On local MinIO that's ~14 ms; **on a real bucket it is your store's GET RTT** (tens of
ms), on every read.

That is the honest Tier-3 trade: **read throughput scales horizontally, but read latency is
bounded below by one head GET.** Removing that per-read round trip is exactly what Tier 4
buys — the head arrives on a stream instead of being fetched (§6).

> **Measure it, don't time it.** Datahike caches connections on `[store-id, branch]` — not on
> the writer backend — so a "reader" pool opened in the same JVM as the writer is handed the
> **writer's own connection**, derefs its in-memory atom, and reports sub-millisecond reads
> with zero S3 traffic. It looks fantastic and proves nothing. `reader-demo` therefore gives the
> reader its own connection registry, so it is a genuine second node — and `bin/reader-gets`
> checks the wire rather than the clock. If you benchmark a reader yourself, do the same.

`docker compose --profile tier3 up --build` runs it as two processes (writer :8888, reader
:8889 with `SAAS_ROLE=reader`); create tenants on the writer, read them from the reader.

## 6. Tier 4 — LMDB tier bounds the cold-read tail

When the working set exceeds RAM, an in-memory-cache miss falls through to the object store
(~tens of ms). Tier 4 puts a **local LMDB frontend over the S3 backend** (`konserve.tiered`):
a cold miss hits LMDB (sub-ms local disk) instead of S3, so the read *tail* is bounded by
local storage, not the network. Immutable content-addressed nodes make the frontend
never-stale; it's a pure config swap (`:store {:backend :tiered :frontend-config {:backend :lmdb}
:backend-config {:backend :s3}}` — the `-config` suffixes matter, or `:backend` collides with
`:backend :tiered`).

`bin/tier4-demo` models the shipped topology: the **writer** holds S3 directly, and the
**reader** is tiered `{lmdb, s3}` with **`:write-policy :frontend-only`** — a cache that never
writes the writer's S3, so its LMDB starts **empty**. Each scan runs in a fresh JVM (cold
in-memory cache), with the S3 backend at +40 ms:

| cold scan (5000-note index) | time | vs S3-only |
|---|--:|--:|
| reader scan 1 — LMDB cold → S3, fills the cache | 979 ms | 2.5× |
| **reader scan 2 — LMDB warm** | **586 ms** | **4.2×** |
| S3-only, no local tier (+40 ms RTT) | 2447 ms | — |

**4.2× once warm.** Even the *first* scan beats S3-only, because within one scan the first read
of a node populates LMDB and later re-reads hit it locally. The gap widens with real cloud RTT
and larger working sets: the S3-only tail grows with every cold node fetch, while the LMDB tier
stays on local disk. In a live Tier 4 the kabel stream also **pushes each commit's nodes into
the reader's LMDB** (konserve-sync), so the cache warms without waiting for a read to miss.

> ⚠️ **`:write-policy :frontend-only` is load-bearing when you measure this.** Omit it and konserve
> defaults to `:write-through`, so the writer's own commits fill the LMDB — the "cold" scan is then
> served by a cache it got for free, and you have measured local disk vs network, not Tier 4. (The
> tell is a frontend key-count equal to the backend's.)

> `datahike-lmdb` is on Clojars (0.1.8, pinned in the `:lmdb` alias) — it and `konserve-lmdb`
> are both **beta**, so treat the LMDB tier accordingly.

### Reclaiming the LMDB cache — frontend GC on a streaming replica (`tier4-gc-demo`)

konserve.tiered has no eviction, so a replica's LMDB only grows. Reclaim it with
**`konserve.gc/sweep!` on the frontend store**: delete-only against a reachable whitelist, so it
needs no full tree locally — just the reachable set (the *mark*), computed against the shared
backend or **published by the writer**, which already computes one during its own S3 GC.

`tier4-gc-demo` runs the real thing: a **kabel writer** over the shared S3, and a **streaming
replica** on a tiered `{lmdb, s3}` store with `:write-policy :frontend-only`, whose LMDB starts
empty and warms from the stream.

| | frontend (replica LMDB) | backend (shared S3) |
|--|--:|--:|
| replica connects | 39 (warmed by the handshake) | 57 |
| writer churns 1000 updates, replica follows the stream | **920** | 938 |
| after `sweep!` on the frontend | **37** | 938 (**untouched**) |

**96% of the replica's cache reclaimed** (883 of 920 keys; only 41 were reachable), the shared S3
**unchanged** — its garbage is the writer's job — and the replica still reads all 2000 rows *and*
sees the final churn, so it is genuinely current, not sitting on a stale snapshot.

> ⚠️ **The stream is what makes this measurable at all.** A tiered reader that merely `deref`s is
> **frozen**: the branch head is a *mutable* key, `:read-policy :frontend-first` serves it from the
> LMDB cache, and the reader re-reads its own stale copy forever. It would cache exactly one
> generation and report a tidy, meaningless reclaim number. That is why Tier 3 forbids LMDB
> ([ladder.md](ladder.md)) and why Tier 4 delivers the head over the **stream** instead of
> re-reading it. The demo asserts the replica's `max-tx` matches the writer's before it believes
> any of the numbers above.

So the Tier-4 GC story is: **the writer GCs S3; each replica sweeps its own LMDB against the
writer's reachable set** — reusing konserve.gc + datahike machinery, no new eviction subsystem.
(Wipe + re-warm is the zero-dependency fallback.) `d/gc-storage` on the replica's tiered store
would not reach S3 either — under `:frontend-only`, konserve's tiered deletes and `-keys` are
frontend-only — but sweeping the frontend directly says what you mean under any policy.

> The demo reaches datahike's private reachability walk to stay self-contained; for real use
> the reader consumes the writer's published reachable set, so datahike exposing a public
> `reachable-keys` would make this a clean two-liner.

## 7. Cold start — what a short-lived reader pays (`coldstart`)

Tiers 3 and 4 both assume a **long-lived** process: one keeps a warm node cache, the other a warm
local LMDB. A lambda has neither — it opens a store, answers one request, and dies. This bench
measures that case, and which candidate fix actually moves it.

Not a cost question. At $0.40/M ([cost-model.md](cost-model.md)) a hundred GETs cost $0.00004.
What a cold reader pays is **round trips**, in two kinds that respond to opposite fixes:
*depth-serial* (root→branch→leaf within one lookup, bounded by tree depth) and *breadth-serial*
(N independent lookups issued one after another, bounded by the working set).

```bash
docker compose --profile tier1 up -d
bin/latency-proxy 20
MINIO_PORT=19000 SAAS_TIER=tier1 clj -M:bench -m datahike-saas.coldstart compare
```

### Concurrency is the whole game (`fanout`, +20 ms)

The **same 197 keys**, only the number in flight changes:

| in flight | ms | ms/object | speedup |
|--:|--:|--:|--:|
| 1 (today) | 5843.6 | 29.66 | 1.0× |
| 4 | 1526.2 | 7.75 | 3.8× |
| 16 | 468.8 | 2.38 | 12.5× |
| 64 | **357.0** | 1.81 | **16.4×** |
| 128 | 453.2 | 2.30 | 12.9× |

Nothing about *what* is fetched changes — the spread is pure serialization. On localhost the same
spread is 6.3×, so it **widens** with real object-store latency. konserve-s3 implements neither
`PMultiReadBackingStore` nor `PMultiWriteBackingStore`, so konserve's `sync-keys-to-frontend` and
`konserve.gc/sweep!` both take their serial fallback today.

### Preloading everything is 8–16× slower than not bothering (`preload`, +20 ms)

Not a proposal — it is what datahike does **today** for a `:tiered` store: `ready-store :tiered`
runs konserve's `populate-missing-strategy` on connect, hardcoded, with no strategy option.

| issues | naive ms / GETs | preload connect ms / GETs | then query ms / GETs |
|--:|--:|--:|--:|
| 100 | 386.3 / 7 | 3179.0 / 56 | 3.2 / **0** |
| 400 | 735.8 / 21 | 11836.4 / 392 | 1.0 / **0** |

The warm query really is free. Getting there is not — and it is paid *twice*: `connect` GETs run
~2× the object count, because `perform-sync` enumerates with `-keys` (konserve's `list-keys` opens
every blob to read its metadata — 6.0 s on its own here) and then re-fetches every key.

### 80% of a full preload is garbage (`live`)

Same tenant, before and after `d/gc-storage`: **197 → 40 objects**. Enumerate-the-bucket fetches
the garbage too; `konserve.tiered/perform-walk-sync` (root keys + a walk-fn, never calls `-keys`)
would fetch only the live set — but no datahike-side walk-fn exists to feed it yet.

### Budget-bounded BFS warm beats full preload by 27× (`warm`)

`d/warm-db` — prototyped in this repo, now **released in datahike** (0.8.1779, experimental) —
walks the tree from the **root**, fetching each level
concurrently, bounded by a `:depth` policy (`:interior` / `:with-leaves` / an integer) and a
node `:budget`. 400 issues, +20 ms, three phases counted separately:

| strategy | connect | warm | query | **total** |
|---|--:|--:|--:|--:|
| naive | — | — | 735.8 ms / 21 GETs | **735.8 ms** |
| `:with-leaves` budget 2000 | 140.9 / 1 | 246.3 / **37** | 50.7 / **0** | **437.9 ms** |
| `:with-leaves` budget 8 | 114.0 / 1 | 41.9 / 8 | 385.4 / 12 | 541.2 ms |
| tiered `{memory,s3}` preload | 11836.4 / **392** | — | 1.0 / 0 | **11837 ms** |

Same end state as the full preload — **query at 0 GETs** — for 37 fetches instead of 392.
Two reasons, and only one is concurrency: it **walks**, so it touches only *reachable* nodes
(37, against 40 live and 197 total) and never enumerates the bucket. It gets the walk-sync
property by construction rather than as a separate strategy.

> A fixed `perform-sync` (walk-based *and* parallel) would land near this. The warm's durable
> advantage over that is the **budget** — it degrades on a store too large to hold rather than
> refusing to finish.

**The budget-8 row is the cliff-freedom claim as a measurement**: a partial warm gives a partial
saving (query 21 → 12 GETs) and the total lands *between* naive and fully-warmed. No step, no
mode switch. `:with-leaves` with a budget above the object count simply runs out of frontier —
"preload everything" is not a separate mode, it is what the same loop does on a small store.

> `:interior` fetches **0** at these sizes, correctly: the eavt tree is **height 1**, so the
> interior *is* the root, which `:fuse-index-roots?` already delivers with the connect's single
> GET. Our tenants are structurally too small to have an interior layer at branching factor 512.

To exercise a multi-round walk, re-seeded at **branching factor 32** so height 3 is reachable in
~20k datoms (the walk is bf-independent; only the interior/total *ratio* is not):

| depth | fetched | by level | rounds |
|---|--:|---|--:|
| `:interior` | 93 | `[5 88]` | 2 |
| `:with-leaves` | 1504 | `[5 88 1411]` | 3 |
| `1` | 5 | `[5]` | 1 |

`:interior` stops exactly at the leaf boundary with no rule enforcing it — leaves are level 0, so
BFS terminates there by itself.

> ⚠️ **The 1/branching-factor estimate is optimistic by 2×.** Measured interior share is
> 93/1504 = **6.2%**, not 1/32 = 3.1%: nodes sit ~50% full (1411 leaves under 88 parents ≈ 16
> children each, not 32), so the *effective* fanout is about half the branching factor and
> `interior/total ≈ 2/bf`. At bf 512 that is ~0.4%, not ~0.2% — for a 95,572-object store, ~370
> interior nodes rather than ~190. Still 2 waves at width 256, so the strategy is unaffected,
> but size a budget from the measured ratio, not the branching factor.

### Small tenants are already optimal (`cold`)

| issues | connect ms / GETs | query ms / GETs |
|--:|--:|--:|
| 5 | 44.2 / 1 | 22.3 / **0** |
| 100 | 43.3 / 1 | 72.5 / 6 |
| 400 | 48.2 / 1 | 295.6 / 20 |
| 1200 | 29.0 / 1 | 421.5 / 57 |

`connect` is **1 GET at every size** — with `:fuse-index-roots? true` the index roots are inlined
in the db record. At 5 issues the query then reads *no nodes at all*: the fused record **is** the
database. Worth stating plainly, because it means the cold-start problem is a
**big-single-database** problem, and db-per-tenant is what dissolves it.

Raw numbers: [doc/results/coldstart.edn](results/coldstart.edn).
