# Blobs and attachments (`:db.type/store-ref`)

*Experimental.* Requires datahike ≥ 0.8.1733 (this template pins 0.8.1735).

Some values have no queryable structure — a screenshot on a bug, a crash dump, a PDF. You
want them **stored and fetched**, not indexed. `:db.type/store-ref` lets a datom **name**
such an object so the database knows it is still in use — and, crucially, so the garbage
collector keeps it alive.

The demo schema (`example.schema`) attaches blobs to issues:

```clojure
{:db/ident :attachment/blob   :db/valueType :db.type/store-ref :db/cardinality :db.cardinality/one}
{:db/ident :attachment/filename     :db/valueType :db.type/string  ...}
{:db/ident :attachment/content-type :db/valueType :db.type/string  ...}
{:db/ident :attachment/size         :db/valueType :db.type/long    ...}
{:db/ident :attachment/storage      :db/valueType :db.type/keyword ...}  ;; :in-store | :s3-direct
{:db/ident :issue/attachments :db/valueType :db.type/ref :db/cardinality :db.cardinality/many
                              :db/isComponent true}
```

The type adds exactly one thing over a plain `:db.type/uuid`: **the GC marks it.** That is
the whole feature. Everything else here follows from one rule.

## The rule: the database is the root set

> **An object lives iff a datom names it.** Retract the datom and it becomes collectable;
> keep history and it stays live, because an `as-of` read can still reach the datom that
> names it.

So `delete-tenant!` erases a tenant's blobs *with* the tenant, and GDPR erasure stays
complete by construction — there is no orphaned blob store to sweep separately. All of the
below is verified end-to-end in `test/datahike_saas/attachments_test.clj` and the
integration tests.

## Where the bytes live is your choice

The same `:attachment/blob` type covers two deployments, and `example.attachments` implements
both. Every function takes a plain connection or db — no pool coupling.

### In-store — bytes in the tenant's own konserve store

```clojure
(require '[datahike-saas.example.attachments :as att])
;; conn = a tenant connection (dev/user: (conn "acme"))
(def bid (att/attach! conn issue-id {:filename "shot.png" :content-type "image/png" :bytes ba}))
(att/fetch conn bid)   ;; => byte-array, the exact bytes back
```

- ✅ **`d/gc-storage` reclaims it** once no datom names it — portably, on every backend.
- ✅ **`delete-tenant!` erases it** with the database. One store, one lifetime.
- ✅ **Portable** — the local `:file` setup behaves exactly like S3.
- ⚠️ **The bytes proxy through your JVM** (`client → server → store`), and you forgo S3-native
  range requests, resumable multipart, and CDN. Fine at moderate size; **S3-direct is the
  only thing that scales.**

Over HTTP (base64 in the JSON body — enough to demo the round-trip without multipart):

```bash
B64=$(base64 -w0 screenshot.png)
curl -XPOST localhost:8899/t/acme/issues/$ISSUE_ID/attachments \
  -H content-type:application/json \
  -d "{\"filename\":\"screenshot.png\",\"content-type\":\"image/png\",\"data\":\"$B64\"}"
# => {"blob-id":"6a55…"}
curl -s localhost:8899/t/acme/attachments/6a55… | jq -r .data | base64 -d > out.png
```

### S3-direct — the browser PUTs straight to a presigned URL

The bytes **never touch your JVM**, which is the entire point at any real size. The browser
`PUT`s to `s3://bucket/tenant/<slug>/blobs/<id>` with a presigned URL and a `Content-Type`;
you transact the id:

```clojure
(att/record-external! conn issue-id {:blob-id content-id :filename "big.pdf"
                                     :content-type "application/pdf" :size 1048576})
```

Datahike cannot delete from a bucket it doesn't own, and does not pretend to. It gives you
the **mark** — the hard half — and you sweep your own prefix:

```clojure
(let [live (att/live-blob-ids @conn)]          ;; every id still named, across branches + history
  (doseq [id (list-your-prefix bucket (str "tenant/" slug "/blobs/"))]
    (when-not (live id) (delete-object bucket id))))
;; att/garbage-ids is exactly this subtraction over a list you pass in.
```

`live-blob-ids` (datahike's `gc/reachable-store-refs`) returns the live set **across all
branches and through retained history**, honouring `remove-before` exactly as index nodes do.
Marking an in-store id costs the local sweep nothing, so you can **mix both deployments
freely**.

## The write window (in-store only)

Writing the object and naming it in a *later* transaction leaves a gap in which nothing names
it — and by the rule above, an object nothing names is garbage. If a collection runs in that
gap it may take it. `attach!` holds a fence across both writes:

```clojure
(guard/with-unreferenced-writes store-id       ;; store-id derived from the conn
  (k/bassoc store id bytes {:sync? true})
  (d/transact conn [{:issue/attachments [{:attachment/blob id …}]}]))
```

For a browser upload the object exists in S3 before any transaction names it, so instead:
**transact the attachment entity on upload** (the id is named from the moment it lands), or
**sweep with an age floor** (only delete objects older than your longest upload-to-transact
gap). Both are standard practice.

## The id is content, not a pointer or a path

`att/attach!` ids a blob by `datahike.blob/blob-id` — a `hasch` content hash. This is not
stylistic:

- **Not a mutable pointer.** A store-ref is dereferenced on read, *including an `as-of` read
  of an old transaction*. A content hash makes the reference *be* the content, so an old
  reference necessarily yields the old bytes. A mutable name would be a second mutable cell,
  and one datahike can neither see nor protect.
- **Not a path.** Paths move and objects at a path get overwritten. Store the path as an
  ordinary indexed `:db.type/string` datom beside the id — the git model (blobs by content;
  trees map names to hashes). A rename then touches one datom and every historical reference
  still resolves.
- **A UUID, not a string.** datahike's index order is the value's `compareTo`, and
  `(compare #uuid"…" "a-string")` throws. So the *reference* is a UUID; anything
  string-shaped (paths, foreign ids) lives in its own attribute.

## What this is NOT for

**Structured data.** If you would ever filter or join on something *inside* the value, it is
a document, not a blob:

| what it is | where it goes |
|---|---|
| structure you **query** | datoms — `datahike.experimental.unstructured` shreds a nested map into entities |
| structure you **search** | a secondary index — `:db.secondary/*` |
| bytes you **fetch** | `:db.type/store-ref` |

datoms are already sparse — an entity has whatever attributes it has — so storing a document
as an opaque value buys no flexibility you didn't already have; it only costs you the indices.

See datahike's own [`doc/store-refs.md`](https://github.com/replikativ/datahike/blob/main/doc/store-refs.md)
for the full type contract (including the schema shapes it *rejects* to keep the mark sound).
