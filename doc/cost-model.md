# Cost model — what a tenant actually costs

The point of db-per-tenant on object storage: a tenant is *a few objects in a bucket*, and one
small VM serves thousands of them. This page turns the measured write path (§1 of
[benchmarks.md](benchmarks.md): **1 PUT/commit** with the default config) into dollars.

Prices are **list, checked July 2026**, and linked. Treat them as ratios, not quotes — and use
the [benchmark harness](benchmarks.md) to measure your own workload against your own bucket.

> Object-store operations cost about **one cent per tenant per month**. On AWS the bill is
> dominated by **egress to your users** — which has nothing to do with the bucket. See
> [what actually dominates](#what-actually-dominates).

## Picking a store

| provider | PUT ($/M) | GET ($/M) | storage ($/GB·mo) | egress | small-object writes |
|----------|----------:|----------:|------------------:|:------:|:--------------------|
| [AWS S3 Standard][aws] | 5.00 | 0.40 | 0.023 | $0.09/GB¹ | median "tens of ms" ([AWS][aws-perf]); p90 1 KB PUT <38 ms |
| [Tigris][tigris] | 5.00 | 0.50 | 0.020 | **free** | **fastest** — small-object optimized; write tail <17 ms |
| [Cloudflare R2][r2] | 4.50 | 0.36 | 0.015 | **free** | **slow — avoid for writes**: p90 1 KB PUT >340 ms² |
| [Hetzner Object Storage][hetzner-os] | **free** | **free** | ~0.005³ | 1 TB incl. | **poor fit** — HDD-backed, see below |

¹ First 10 TB/mo — **but this is $0.00 for same-region S3 → EC2.** See below.
² Small-object latency figures are from [Tigris's benchmark][tigris-bench] — a vendor comparing
  itself to rivals, so weight accordingly; the AWS median is from AWS's own docs. Cloudflare
  shipped [R2 Local Uploads][r2-local] (Feb 2026), which cuts *cross-region* write latency ~75%.
³ **€4.99/mo per bucket base**, including 1 TB storage + 1 TB egress; overage ≈ €5/TB·month.

**Two things worth knowing, because they're counterintuitive:**

- **Hetzner charges nothing per request** — their docs say *"S3 operations such as PUT, GET, or
  DELETE"* are free ([docs][hetzner-docs]) — **but its Object Storage is the wrong engine for this
  workload.** It's HDD-backed, *"not designed for applications that require single-digit
  millisecond response times"*, *"optimized for files of approximately 1 MB and larger"*, and
  explicitly advises against *"large numbers of very small files"* ([Hetzner][hetzner-perf]). A
  datahike commit is a few-KB object. Free PUTs don't help if each one is slow.
- **Tigris is not cheaper than S3 on requests** — same $5.00/M PUT, and its GETs are *dearer*
  ($0.50/M vs $0.40/M). Its wins are **latency on small objects** and **free egress**.

**⇒ The pairing we'd actually pick: a Hetzner VM + a Tigris bucket.** Cheap compute with
generous included user traffic, small-object writes that suit a commit, and Tigris's free egress
is exactly what makes the cross-cloud hop cost nothing. Numbers below.

**Free tiers:** R2 gives 10 GB + 1M Class A + 10M Class B ops/month; Tigris gives 5 GB + 10k
Class A + 100k Class B.

## The per-tenant model

```
cost = commits × PUT$ + cold_reads × GET$ + storage_GB × GB$
```

Grounded in this repo's own measurements ([benchmarks.md](benchmarks.md)):

| input | value | source |
|---|---|---|
| PUTs per commit | **1.00** | `bin/put-count`, MinIO trace, default config |
| heap per tenant (connection hot) | **0.09 MB** @ 5 issues · **1.30 MB** @ 100 issues | `workload scale` — scales with tenant DATA, so the fleet heap below is a floor, not a bound |
| storage per tenant | ~7.5 KB + ~2.5 KB/note | `workload scale` |

**Tenant profile** — a small B2B team, and the number you should argue with first:

| | |
|---|---|
| users per tenant | 10 |
| per user per working day | 100 page views, 10 writes |
| working days/month | 20 |
| **⇒ per tenant·month** | **20,000 app reads, 2,000 commits** |
| notes per tenant | 1,000 → ~2.5 MB stored |

**Most reads never reach the bucket.** A warm read is a hashmap lookup in datahike's caches
(measured p50 ~2 ms, no object I/O); only a cache miss or a fresh connection fetches objects. We
assume **5% of reads are cold**, ~3 objects each → 3,000 GETs/tenant·month. That's the softest
assumption here — and also the one that barely matters, since GETs are ~10× cheaper than PUTs.

### Result: ~1 ¢ per tenant per month

| provider | 2,000 PUTs | 3,000 GETs | 2.5 MB stored | **$/tenant·mo** |
|---|---:|---:|---:|---:|
| AWS S3 | $0.0100 | $0.0012 | ~$0.0001 | **$0.011** |
| Tigris | $0.0100 | $0.0015 | ~$0.0001 | **$0.012** |
| Cloudflare R2 | $0.0090 | $0.0011 | ~$0.0000 | **$0.010** |

Whatever you pick, storage ops are a rounding error — **as long as a commit stays at 1 PUT.**
That's what the config knobs buy.

## The fleet: a back-of-envelope you can use

Compute: [AWS m7g.large][ec2] (2 vCPU / 8 GB) at **$0.0816/h ≈ $59.57/mo**;
[Hetzner CPX41][hetzner-cloud] (8 vCPU / 16 GB) at **≈ €33/mo**, with a generous included traffic
allowance (20 TB on EU locations — check your region). User-facing egress assumes a **20 KB
average response**.

### 1,000 tenants

| | all-AWS | Hetzner VM + Tigris |
|---|---:|---:|
| load | 35 reads/s + 3.5 writes/s — **one node handles it** | same |
| heap | bounded pool (`SAAS_MAX_HOT`) — tracks the *active* set, not the fleet | same |
| compute | 1 × m7g.large — **$60** | 1 × CPX41 — **$36** |
| object store (2M PUTs, 3M GETs, 3 GB) | **$11** | **$12** |
| egress to users (400 GB) | **$27** | **$0** (included) |
| **total** | **≈ $98/mo → $0.098/tenant** | **≈ $48/mo → $0.048/tenant** |

> **Heap is governed by how many tenants are HOT, not how many exist.** A hot connection holds
> that tenant's in-memory index — ~13 MB at this page's 1,000-note profile ([§3b](benchmarks.md)) —
> so keeping the whole fleet open would be ~13 GB at 1,000 tenants and ~130 GB at 10,000. The pool
> is therefore **bounded** (`SAAS_MAX_HOT`, default 512): heap tracks the concurrently-active set,
> and the compute lines above assume that. Set it from your *active* tenant count, not your fleet
> — an evicted tenant pays ~+52 ms on its next request.

### 10,000 tenants

| | all-AWS | Hetzner VM + Tigris |
|---|---:|---:|
| load | **347 reads/s** + 35 writes/s — **past one node's ceiling** (§2) → **Tier 3**: 1 writer + 3 readers | same |
| heap | bounded pool, per the note above — the fleet does not have to fit in RAM | same |
| compute | 4 × m7g.large — **$238** | 4 × CPX41 — **$144** |
| object store (20M PUTs, **200M GETs**, 25 GB) | **$181** | **$201** |
| egress to users (4 TB) | **$351** | **$0** (within allowance) |
| **total** | **≈ $770/mo → $0.077/tenant** | **≈ $345/mo → $0.035/tenant** |

> **Tier 3 changes the GET line, and the model has to follow.** A direct-bucket reader re-reads the
> branch head on **every** deref — measured at exactly **1.00 GET/read** ([§5](benchmarks.md),
> `bin/reader-gets`). So the moment you scale reads with Tier 3, GETs stop tracking your *cold-miss*
> rate and start tracking your *total read* rate: 10,000 tenants × 20,000 reads = **200M GETs**, not
> the 30M a single node would do. That is +$69/mo on AWS — still not the dominant line, but it is
> the one term Tier 3 makes worse, and it is why Tier 4 (head over the stream, zero GETs per read)
> is the answer to a read-heavy fleet rather than more Tier-3 readers.

Per-tenant cost *falls* as the fleet grows: the VM amortizes, and nothing else is per-tenant.
No connection pool, no provisioned database, no per-tenant server. That is the whole economic
argument.

The Hetzner+Tigris column pays a little more for the bucket and saves the entire egress line —
and Tigris's free egress is what makes reading that bucket from a non-Tigris VM free.

## What actually dominates

**Not the bucket.** At 10,000 tenants on AWS:

```
egress to users   $351   ← the largest line, and it is not object storage
compute           $238
object store      $113
                 ─────
                  $702
```

Three consequences:

1. **Same-cloud egress is free, so "egress-free storage" is not the lever you'd think.** S3 → EC2
   in the same region is **$0.00/GB** ([AWS][aws-dt]) — the bucket only ever talks to your VM. Free
   egress starts paying when the bucket is in a *different* cloud than the compute, which is
   exactly why **Hetzner VM + Tigris bucket** works: the cross-cloud hop is free in that direction.
   (A Hetzner VM reading an *S3* bucket would pay S3 egress on every cold read.)

2. **Watch the NAT gateway.** That $0.00 assumes an [S3 VPC gateway endpoint][aws-dt]. Route bucket
   traffic from a private subnet through a **NAT Gateway** instead and you pay ~$0.045/GB in data
   processing on traffic that should have been free.

3. **The big AWS line is user egress, not storage.** $351 of a $702 bill is bytes going to
   browsers. Anything that shrinks responses (compression, pagination, a CDN) beats anything that
   shrinks the bucket.

## The write-amplification lever: what it buys, and what it doesn't

The default config commits in **1 PUT** ([§1](benchmarks.md)). Turning the knobs off multiplies
exactly one term:

| config | PUTs/commit | AWS PUT bill @ 10k tenants |
|---|:---:|---:|
| **default** (fusion + diff-buf, no commit-graph) | **1.0** | **$100/mo** |
| `commit-graph?` on | 2.0 | $200/mo |
| fusion off | 4.0 | $400/mo |

At this scale the knobs are worth a few hundred dollars a month — real, but not the dominant term.

**Their bigger payoff is latency.** Index-root writes are *sequential* (a child must land before
its parent), so a commit pays for its number of **sequential PUT waves**, not its raw PUT count —
and fusion removes a whole wave. Measured through a +40 ms proxy ([benchmarks.md](benchmarks.md)
§4): commit p50 **75 ms** with fusion, **119 ms** without — one extra round trip on *every* commit.
That's why they're on by default.

### `:commit-graph?` is a feature you're declining, not just a cost

All three cost you something, and the costs are different in kind.

**Fusion** trades a little **structural sharing** for PUTs. Inlining each index's root into the db
record saves one PUT per index touched (2–6 per commit) and a GET per index on cold open — but the
record grows, and two commits that didn't change an index can no longer point at the *same* root
object. That only bites if you **retain** db records (`:commit-graph?` on), and it is minor.

**diff-buf** trades **CPU**, not IO. Buffered diffs are projected onto nodes as they load, so a
cold full-range scan does roughly 2× the node work
([datahike](https://github.com/replikativ/datahike/blob/main/doc/write-amplification.md)) — but
point lookups, counts and warm scans are unaffected, and there are **no extra object-store
requests**. On object storage, spending CPU to avoid PUTs is usually the trade you want. What it
*does* cost you is optionality: `:diff-buf-size` shapes the on-disk representation, so it is fixed
at database creation and cannot be removed without recreating the store. It is also the newest of
the three — promising, but wanting more testing before anyone calls it stable.

**`:commit-graph? false`** is a different animal: it's a **feature trade**, worth understanding
before you copy the config.

The commit graph is Datahike's **provenance chain** — the thing that makes it git-like. Keeping it
buys [branching and versioning](https://github.com/replikativ/datahike) (`branch!` from a
commit-id, ancestry walks, merges), the tamper-evident audit chain, and `dh://…?commit=` references
that pin an exact content-addressed commit. If any of that is part of your product — audit trails,
"restore this tenant to last Tuesday", per-tenant sandbox branches, a review-then-merge workflow —
**leave it on.**

And leaving it on is cheaper than it sounds, because Datahike uses **structural sharing**: a
retained old commit doesn't store a copy of the database, it shares every unchanged node with the
current tree. You pay only for the nodes that actually diverged. As a way to keep history, that's
close to optimal — it's the same reason a git repo isn't N copies of your source.

What it costs, concretely, is two things:

- **one extra PUT per commit** (the provenance record) — the table above;
- **retained garbage.** Each commit record pins *that commit's* index roots, so superseded nodes
  stay reachable while it does. You reclaim them by pruning the chain (`gc-storage`'s
  `remove-before`, or background GC's `:history-window-ms`), not by running GC more often. With
  the graph off, the lineage ends at the head and superseded roots are collectable immediately —
  which is why the churn test reclaims 99.7% ([§3](benchmarks.md)).

This template turns it **off** because a per-tenant issue tracker doesn't branch tenant databases
and doesn't need commit-level provenance — so it's paying for nothing. That's a judgment about
*this* demo's requirements, not a recommendation to disable versioning in general. Note that
`keep-history?` is a **separate** axis: time travel (`as-of`, `history`) lives in the temporal
indices and works fine with the commit graph off.

## Measure your own

Every number above rests on a tenant profile we made up (100 views + 10 writes per user per day)
and on our workload's shape, not yours. A consumer app with 10× the reads and a tenth of the
writes lands somewhere else entirely — so the model is parameterized, and **the repo ships the
harness to check it.** Point the benchmarks at your own tier and bucket:

**Against your real bucket** — latency, density and object growth, at your rates:

```bash
export SAAS_TIER=tier2                              # note the export
export S3_ENDPOINT=... S3_BUCKET=... AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
clj -M:bench -m datahike-saas.workload mixed        # read/write latency at your rates
clj -M:bench -m datahike-saas.workload scale        # tenants/node for your schema
clj -M:bench -m datahike-saas.workload compare      # stored-object growth per commit
```

**PUTs/commit needs a request log, not a stopwatch** — a commit overwrites the branch head at a
*fixed* key, so the in-process view can't see it. `bin/put-count` and `bin/reader-gets` get it by
grepping MinIO's trace, which means they are **Tier-1 only by construction**:

```bash
bin/put-count       # PUTs/commit for your commit shape   (local MinIO)
bin/reader-gets     # GETs/deref for a Tier-3 reader      (local MinIO)
```

Your commit *shape* is what determines the count, and that doesn't change with the bucket — so
measuring it on MinIO transfers. To confirm on the real thing, read PUT/GET counts off your
provider's request metrics (S3 → CloudWatch or server access logs).

Between them you get the three inputs the model actually needs: **PUTs/commit**, **cold-read
fraction**, and **tenants/node**.

Not modeled at all: load balancer, backup/DR replication, monitoring, log storage, support plans,
IPv4 addresses, VAT. Flat-ish, but on a small fleet they can match the compute line. The
10,000-tenant row is extrapolated from density measured at 1,000; the ~300 ops/s/node ceiling *is*
measured (§2), and it's what forces the reader fan-out.

[aws]: https://aws.amazon.com/s3/pricing/
[aws-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html
[tigris-bench]: https://www.tigrisdata.com/blog/benchmark-small-objects/
[r2-local]: https://blog.cloudflare.com/r2-local-uploads/
[hetzner-perf]: https://docs.hetzner.com/storage/object-storage/faq/general/
[aws-dt]: https://aws.amazon.com/blogs/architecture/overview-of-data-transfer-costs-for-common-architectures/
[ec2]: https://aws.amazon.com/ec2/pricing/on-demand/
[tigris]: https://www.tigrisdata.com/pricing/
[r2]: https://developers.cloudflare.com/r2/pricing/
[hetzner-os]: https://www.hetzner.com/storage/object-storage/
[hetzner-docs]: https://docs.hetzner.com/storage/object-storage/overview/
[hetzner-cloud]: https://www.hetzner.com/cloud/
