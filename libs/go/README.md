# Confined object stores (`libs/go`)

The object counterpart of the row-confined Postgres `Store`. A `Store` binds
every call to one `Binding` and addresses objects by logical reference; it
builds the key, and reads it back, itself. A caller never passes or receives a
key.

```
github.com/codefly-dev/service-object-storage/libs/go                   Store, Binding, Maintenance, S3SessionPolicy
github.com/codefly-dev/service-object-storage/libs/go/objectkey         the key convention and its codec
github.com/codefly-dev/service-object-storage/libs/go/objectscopetest   Prove and RequireLayout, for a module's own suite
```

It speaks to the gateway over `codefly/storage/v0`, like every other client.
The Store links no cloud SDK. To issue scoped MinIO read sessions, pass
`WithReaderSessions(miniosession.Issuer{...})` at the composition root. The
issuer uses the MinIO SDK and the Store passes the resulting credential to
the gateway for Get, Head and listing.

## The words

They are the Postgres library's, on purpose: one adapter derives one decision
and both stores honour it.

| Word | Meaning here |
|---|---|
| **partition** | the wall no call crosses; the second segment of every key |
| **scope** | one unit inside a partition; `Reach` is the scopes a call may touch |
| **principal** | whose objects, as `kind:id` |
| **mode** | `Strict` or `Privileged`. There is no third value |
| **access** | `Read` or `ReadWrite` |
| **actor** | who is executing; recorded, never decided on |
| **base** | the store's own name inside the bucket and prefix the gateway was given |

`Binding` is declared here, field for field what the Postgres `Binding` is,
because neither library may import the other. It belongs in a package both can
import. `Shared` is the one field with no row counterpart: a shared row still
has a principal, a shared object has none.

## The key convention

```
<base>/<partition>/<scope>/<kind>.<id>/<name…>     one principal's objects
<base>/<partition>/<scope>/_shared/<name…>         the scope's own objects
<base>/<partition>/<scope>/_sha256/<digest>        content, named by its SHA-256
<base>/<partition>/<scope>/<kind>.<id>/_staging/<id>   an upload in flight
```

- **Base**: one segment, `[a-z0-9][a-z0-9-]{0,62}`. One segment, so no base is
  a prefix of another. A module with several stores names them
  `<module>-<store>`.
- **Every other segment** is written by one function over `A-Z a-z 0-9 - .`,
  with every other byte as `_hh` (two lowercase hex digits). `_` is the escape,
  so it is always escaped; a `.` is escaped first and last in a segment. No
  identifier can therefore spell `/`, `..`, an empty segment or a reserved one
  (`_` followed by a letter that is not a hex digit).
- **A principal** `kind:id` is the segment `kind.<id>`.
- **A name** is segments joined by `/`. Every segment but the last carries a
  trailing `_`: `a/b/c` is `a_/b_/c`, and `a` is `a`. A name is then never both
  an object and the directory of another, which stores disagree about (the
  pinned MinIO serves both and lists only the first).
- **Refused**: empty, `.` and `..`, control characters, text that is not
  UTF-8, a segment over 240 encoded bytes, a key over 900 bytes (stores take
  1024; the rest is the gateway's own prefix).
- **Not normalized**: bytes are encoded as they come. Two Unicode spellings of
  one visible name are two names. Normalize names that people type before they
  reach the store.
- **Case is kept**, so the store must keep it. The integration suite pins that
  the MinIO this repo builds does.

A key holds only `A-Z a-z 0-9 - . _ /`: nothing a signed URL percent-encodes
and nothing an access policy reads as a wildcard or a variable.

The encoding never changes. `objectkey`'s golden vectors are the pin; a
different convention is a different base.

## What a Binding reaches

| | own slot | another principal's | `_shared` | `_sha256` |
|---|---|---|---|---|
| **Strict** | read, list, write | nothing | read, list; write only with `Shared: ReadWrite` | read by digest; no listing; write through `PutContent` |
| **Privileged** | read, list, write | read, list, write | read, list, write | read, list; write through `PutContent` |
| **`Access: Read`** | no write, either mode | | | |
| **empty reach** | `ErrNoReach`: nothing, never everything | | | |

Nobody crosses a partition: a reference cannot name one. Content is deleted by
`Maintenance` only, since only the application knows what still references a
digest.

Every refusal is decided from the reference and the `Binding` alone, before
the gateway is called, so a refusal says nothing about what exists.

## A handler, strict: upload and presign

```go
// Composition root. The client is dialed once and given to the Store; no
// handler ever sees it.
store, err := objectstorage.NewStore(storagev0.NewObjectStorageClient(conn), "files", binder)

// A handler. The Binding was derived from the verified caller by the binder;
// the handler names no mode.
func (h *Handler) Upload(ctx context.Context, name string, body io.Reader) (string, error) {
	if _, err := h.store.Put(ctx, objectstorage.Own("uploads/"+name), body, objectstorage.PutOptions{
		ContentType: "application/pdf",
	}); err != nil {
		return "", err // errors.Is(err, objectstorage.ErrReadOnly), ErrInvalidName, …
	}
	// A URL the browser fetches itself. It reads exactly this object, for five
	// minutes, with no credential.
	signed, err := h.store.PresignGet(ctx, objectstorage.Own("uploads/"+name), 5*time.Minute)
	return signed.URL, err
}

// The other direction: the browser uploads straight to the store.
signed, err := h.store.PresignPut(ctx, objectstorage.Own("uploads/"+name), 5*time.Minute)
```

`Own(name)` is the binding's own slot in its only scope. `Shared(name)`,
`Of(principal, name)` and `Content(digest)` name the others, and `.In(scope)`
picks a scope when the binding reaches several.

A presigned URL needs a lifetime, at most the Store's limit (15 minutes unless
`WithPresignLimit` says otherwise). A presigned `PUT` binds the key and the
lifetime and nothing else: the gateway's contract carries neither a size nor a
content type, so its holder may upload anything until it expires. Check what
arrived (`Head`) before recording it.

## A job that is the scope's own principal, privileged: list a scope

```go
// The binder returned Mode: Privileged because the caller is the scope's own
// principal. Nothing in this function asked for it.
func (j *Job) Sweep(ctx context.Context, scope string) error {
	options := objectstorage.ListOptions{}
	for {
		page, err := j.store.ListScope(ctx, scope, options)
		if err != nil {
			return err
		}
		for _, object := range page.Objects {
			// object.Ref says whose it is: Principal, Shared or Content.
			j.consider(object.Ref, object.Size, object.Modified)
		}
		if options.Token = page.Next; options.Token == "" {
			return nil
		}
	}
}
```

## Content: bytes that rows share

```go
info, err := store.PutContent(ctx, scope, body, objectstorage.PutOptions{})
digest := info.Ref.Name                      // record this in the row
body, _, err := store.Get(ctx, objectstorage.Content(digest).In(scope))
```

The body streams to a staging key of the caller's own principal while it is
hashed, and the store copies it in place unless that digest is already there.
The same bytes from two principals are one object. Whoever in the scope can
name a digest can read it, and a strict binding cannot list them: **the row
that holds the digest is what decides who reads the bytes.** Use it for bytes
whose row can be shared or change hands; use the principal's own slot for
bytes that are one principal's for good.

## Maintenance: a principal leaves

```go
// Composition root of the job that owns departures. Not a Store, not built
// from a Binding, and no Store method returns one.
maintain, err := objectstorage.NewMaintenance(storagev0.NewObjectStorageClient(conn), "files")

// Hand over what they had, then remove it: every scope of the partition.
err = maintain.ExportPrincipal(ctx, partition, "human:01HZX3", func(info objectstorage.Info, open func(context.Context) (io.ReadCloser, error)) error {
	return archive.Add(info.Ref, open)
})
erased, err := maintain.ErasePrincipal(ctx, partition, "human:01HZX3")
if len(erased.Failed) > 0 || erased.VersionsMayRemain { /* not done yet */ }

// Their rows are gone or reassigned by now; drop the content nothing references.
_, err = maintain.SweepContent(ctx, partition, scope, 24*time.Hour, rows.References)
```

Erasure is a delete by prefix: one listing per scope, no index, no scan of
anyone else's objects. That is what principals in the key buy. It does not
touch `_shared` or `_sha256`, which hold nothing of a principal's by name.
`VersionsMayRemain` is set on a store that keeps versions: a delete there
leaves the bytes behind a marker, the gateway cannot list or purge versions,
and erasure is complete only once the bucket's lifecycle rule expires them.

`Maintenance` also has `EraseScope`, `ErasePartition`, `SweepStaging`, `Walk`,
`Partitions`, `Scopes`, `Principals` and `Audit`.

## What is enforced, and by what

| Layer | Guarantees | Does not |
|---|---|---|
| **The Store** | every key it builds is inside the Binding; listings are confined on the way out and checked on the way back; a copy checks both ends | confine whoever holds the gateway client. The Store returns it to nobody |
| **The gateway's prefix** (`SOS_PREFIX`) | no client of this gateway reaches another gateway's keys | tell one base, partition or principal from another |
| **A session policy** (`S3SessionPolicy`) | the store itself refuses a credential outside the Binding. Proved on the pinned MinIO with a client that bypasses the gateway and the Store | cover writes or presigning through the configured gateway credential. Each issued read session covers one scope to fit the 2048-byte policy limit |
| **A presigned URL** | one key, one method, one lifetime, signed over the path | bind a size or a type on upload; be revoked before it expires |

`TestMinIOWithoutTheStoreNothingIsConfined` states the second row's limit as a
test: today the gateway client reads any key, and the gateway's credential
reads the whole bucket.

## In a module's own suite

```go
func TestObjectsAreConfined(t *testing.T) {
	objectscopetest.Prove(t, objectscopetest.Proof{Client: client, Base: "files"})
}

func TestEveryObjectIsWhereTheConventionPutsIt(t *testing.T) {
	objectscopetest.RequireLayout(t, ctx, client, objectstorage.Known{
		Partition: knownPartition, Scope: knownScope, // nil accepts anything
	}, "files" /* every base the module uses on this gateway */)
}
```

`Prove` seeds random objects across partitions, scopes and principals with the
gateway client, then acts through the Store under random bindings and holds
every answer, listing, write and refusal to a model that knows nothing about
keys. Pass `Open` to prove a store wrapped in the module's own types.
`RequireLayout` fails on any key the convention did not write, any key under a
partition, scope or principal the module does not know, and anything the
gateway holds outside the declared bases: a layout that predates the
convention shows up there.

## Tests

```bash
go test ./libs/go/...            # the codec, the Store and the proof over the in-memory backend
MINIO_ENDPOINT=127.0.0.1:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin MINIO_BUCKET=sos-test \
  go test -tags integration -count=1 -v ./libs/go/...   # the same, on MinIO; expect 7 more --- PASS, 0 --- SKIP
```

Like `internal/integration`, the MinIO tests skip when `MINIO_ENDPOINT` is
unset and the package still prints `ok`. Count `--- PASS`.

## Not here

- No multipart API: the gateway chooses multipart itself and the client never
  sees parts.
- No object tags and no versions: the gateway's contract lists neither.
- Storage-side read sessions are available for MinIO. GCS and Azure have no
  equivalent issuer. Writes and presigning still use the configured client.
