# The single writer, and what happens when it dies

Every tier here has **one writer per tenant**. That is not an accident of the template — it's
Datahike's model: a connection holds the in-memory db and serializes commits, and the branch head
is a single mutable cell in the store. Two processes writing the same tenant is not "slower", it
is *wrong*.

More precisely: **all writers for a database live in one JVM.** They coordinate in memory, not
through the store — so writer-side maintenance (garbage collection) runs there with them. **Readers
are unconstrained**, which is exactly what Tiers 3 and 4 exploit: as many reader processes as you
like, anywhere. This template keeps every tenant's writer in the app process, so there is nothing
to arrange; what follows is what you'd build to move a *failed* writer's pen to another node.

So the first question any reviewer asks is the right one: **what happens when the writer dies?**

**Reads stay up.** They only need the bucket. Tier-3 readers keep deref-ing the last head; Tier-4
replicas keep serving from their LMDB. Nothing about a writer failure takes reads down.

**Writes stop**, until something else picks up the pen. Picking it up safely is the whole problem,
and this page is the design. **It is not implemented here** — what follows is what you'd build,
and the parts that already exist.

## The failure you're actually preventing

It is *not* "the writer is down". It's **two writers thinking they're the writer**.

A commit ends by writing the branch head at a fixed key. Under `:commit-graph? false` — the config
this template ships — that head write is the *entire* commit: **1 PUT** ([benchmarks §1](benchmarks.md)).
Two writers, W1 and W2, both holding tenant `acme`:

```
W1 reads head@v5, builds v6 in memory
W2 reads head@v5, builds v6' in memory      (it thinks it is the writer too)
W1 PUTs head = v6
W2 PUTs head = v6'                          <- last write wins. v6 is GONE.
```

No error. No conflict. No torn state — the store is perfectly consistent, and it is consistently
missing W1's transactions. **Silent lost updates are the failure mode**, and they are the worst
kind: the system looks healthy.

A network partition, a slow GC pause, a `kill -STOP`, a container that a scheduler declared dead
but which is still running — all of these produce two live writers. You cannot prevent that with
heartbeats. You can only make the *second* writer's commit **fail**.

## The primitive: conditional writes make the bucket the arbiter

S3 (and MinIO) support **conditional PUT**: `If-Match: <etag>` writes the object only if it still
has that ETag, and returns **412 Precondition Failed** otherwise. That is compare-and-swap on an
object.

konserve-s3 already implements it — `put-object-conditional` (`konserve_s3/core.clj`), wired into
the write path behind `:optimistic-locking-retries`. Verified against MinIO:

```
lock created, etag: "50117d884b742022f83bef373d40ba3c"
CAS with the current etag  -> true      the holder can write
CAS with a STALE etag      -> false     the stale writer is REJECTED (412)
```

This is the whole foundation. The object store itself can refuse a stale writer, with no lock
service, no consensus, no ZooKeeper. **You don't need a lease to be *correct*. You need a lease to
be *available*.** Those are two different jobs, and conflating them is how these designs go wrong.

## Design: fence the head, then lease for liveness

### 1. Fencing (correctness) — condition the head write on the head you read

The writer remembers the ETag of the branch head it read. Its commit writes the head with
`If-Match: <that etag>`. Then:

- If the head still has that ETag, nobody else has committed since → the write lands.
- If another writer has committed, the ETag has changed → **412** → this writer has been fenced.

A fenced writer must **fail loudly**: drop its in-memory db, refuse further transactions, and
surface the error to callers. It must *not* retry blindly — that's how you paper over split-brain
and lose data anyway.

**Fencing the head is sufficient — you do not have to fence anything else.** Index nodes are
content-addressed and immutable, so they are *unreachable* until a head points at them. A fenced
writer that already wrote some nodes has produced collectable orphans, not corruption. The branch
head is the **only mutable cell in the store**, which is why a single CAS on it decides the whole
commit, no matter how many node writes preceded it. (That is the same barrier invariant that lets
non-atomic object stores be crash-safe at all — see [ladder.md](ladder.md).)

`:commit-graph? false` sharpens this to its limit: with the provenance record gone, a small commit
*is* one object write — the head, in place ([benchmarks §1](benchmarks.md)) — so the CAS and the
commit are literally the same request. With the commit graph on, fencing still works (the head is
still the only mutable cell), you just also write a provenance object the loser will orphan.

**Note this is correct with zero timing assumptions.** No clocks, no timeouts, no heartbeats. Two
writers can both believe they're primary for as long as they like; only one of them can land a
commit, and the other finds out immediately. Split-brain becomes an *error*, not corruption.

### 2. Leasing (liveness) — so a standby knows when to take over

Fencing alone means: whichever writer commits first wins, and the loser dies. That's safe but
chaotic — you don't want two writers racing every commit and one of them dying at random.

So add a **lease object** per tenant (or per writer node, if a node owns a shard of tenants):

```clojure
;; key: <store-id>_.lease
{:owner   "writer-7f3a"     ; who holds the pen
 :epoch   42                ; monotonically increasing; a fencing token
 :expires 1783929128981}    ; wall-clock ms, advisory only
```

- **Acquire / renew:** read the lease with its ETag; CAS it (`If-Match`) to `{owner: me, epoch:
  epoch+1, expires: now+TTL}`. The CAS is what makes acquisition atomic — the ETag, not the
  clock, is the arbiter.
- **Renew** well inside the TTL (say TTL 30 s, renew every 10 s).
- **Take over:** a standby that sees an expired lease attempts the CAS. If it wins, it is the new
  owner at a higher epoch. If it loses (412), someone beat it — back off and re-read.
- **On acquiring:** the new writer must **re-read the branch head from the store** and rebuild its
  in-memory db. It cannot trust anything it cached.

**The lease is advisory. The fence is authoritative.** If the old writer was merely paused (GC,
`SIGSTOP`, a partition) and wakes up believing it still holds a valid lease, it will try to commit
— and the conditional head write will reject it, because the new writer has moved the head. The
lease decides *who should try*; the head CAS decides *who succeeds*. Get this backwards — lease
only, no fence — and a clock-skew or pause longer than the TTL gives you silent lost updates, which
is precisely the failure we started with.

## What it costs

- **Nothing on the happy path.** A conditional PUT is the same request as a PUT — one round trip,
  same price. The template already commits in 1 PUT; it stays 1 PUT.
- **Lease traffic:** one small GET+PUT per renewal interval per writer, not per tenant if a node
  owns a shard. At a 10 s renew, that is ~8,600 requests/month/node — under a cent.
- **RTO:** bounded by lease TTL + the standby's rebuild. TTL is your knob: 30 s TTL ⇒ writes resume
  within ~30 s of a hard failure. The rebuild is the per-tenant reconnect cost — the
  ~+52 ms/tenant reopen measured in [§3b](benchmarks.md), paid lazily as each tenant is first
  touched, not up front.
- **In-flight commits at the moment of failure are lost.** A commit that never landed never
  happened; the client must retry. This is the same contract as any single-writer system, and the
  reason it's *safe* is the barrier invariant: a head is only written after every node it
  references, so a killed writer leaves collectable orphans, never a dangling pointer
  ([ladder.md](ladder.md)).

## What exists, and what you'd have to build

| piece | status |
|---|---|
| S3/MinIO conditional PUT (`If-Match`, 412) | **exists** — verified |
| konserve-s3 `put-object-conditional` + `:optimistic-locking-retries` | **exists** |
| A commit that is a single object write (fenceable) | **exists** — `:commit-graph? false`, 1 PUT/commit |
| Read-the-head-with-its-ETag, thread it through `commit!` | **missing** — Datahike does not surface the head's ETag |
| Surface a 412 as a typed "you have been fenced" error | **missing** |
| Lease object: acquire / renew / take over | **missing** |
| Standby that rebuilds on takeover | **missing** — mechanically it's `d/connect`, which already re-reads the head |

The gap is narrow and it is **in Datahike, not in the object store**: the store can already refuse
a stale writer. What's absent is threading the head's ETag through the commit path and treating a
412 as a first-class "fenced" condition rather than an IO error.

Until that exists: **run one writer, and make sure it is one.** In practice that means a
scheduler-enforced singleton (a Kubernetes StatefulSet of size 1, `provisioned-concurrency=1`, a
systemd unit) — which is *not* a correctness guarantee (schedulers double-schedule under
partitions), and you should size your risk accordingly. That is the honest state of things, and it
is why this page exists rather than a claim that HA is handled.

## Why not just use a lock service?

You could put etcd/ZooKeeper/DynamoDB in front and take a real distributed lock. It would work. It
also adds a component to operate, pay for, and fail — which is a strange trade for an architecture
whose whole argument is *"the object store is the only stateful thing you run."*

The object store can already do the one atomic operation this needs. Use it.
