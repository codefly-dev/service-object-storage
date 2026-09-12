# service-object-storage

A **server exposing a generic object-storage API** over a uniform gRPC contract,
backed by **S3 / GCS / Azure Blob / MinIO** with a built-in caching layer.

Clients speak only the gRPC API and **never link a cloud SDK**. All
platform-specificity, and every cross-cutting feature (caching first), lives in
this one server — written once, all backends compiled in.

> Design & rationale: [codefly-dev/service-object-storage#1](https://github.com/codefly-dev/service-object-storage/issues/1)

## Why

`service-s3` and `service-minio` were the same engine (MinIO) wearing two
incompatible connection contracts, and every app linked a cloud SDK and held
cloud credentials. This replaces both: the app talks to one gateway, and the
backend is a per-environment choice — **local/test always MinIO**, deployed
names the real backend. Because the app only ever speaks the uniform API, you
**test on MinIO and ship on S3**; the MinIO↔S3 gap is absorbed and tested once,
here, not in every app.

## The API (`codefly/storage/v0`)

The honest intersection across all four backends (the surface OpenDAL /
`gocloud.dev/blob` / Rust `object_store` converged on). Anything backend-specific
(ACLs, storage tiers, object-lock, leases, …) is reached only through `Native`.

| RPC | Purpose |
|-----|---------|
| `Stat` | object metadata (HEAD), conditional |
| `Get` | streaming read; range + conditional; small objects are cache-served |
| `Put` | streaming write; multipart hidden in the backend; conditional (create-if-absent / CAS) |
| `Delete` / `DeleteMany` | delete; batch with per-key results |
| `List` | prefix + delimiter + opaque page token |
| `Copy` | server-side copy where supported |
| `Presign` | time-limited URL the client uses directly over plain HTTP (no cloud SDK) |
| `Capabilities` | machine-readable feature set — introspect before calling |
| `Ready` | probe the backing store: is the bucket reachable with these credentials? |
| `Native` | escape hatch for backend-specific verbs |

Errors are normalized to gRPC status codes (`NotFound`, `AlreadyExists`,
`FailedPrecondition`, `Unimplemented`, …) so clients never parse an S3 XML code.

## Byte path

- **Small objects** are proxied through the gateway and are cacheable.
- **Large objects** use `Presign` — the client transfers directly to/from the
  backend over plain HTTP (still no cloud SDK). Presign is GET-biased: writes
  should route through `Put` so the cache always invalidates.

## Cache

A two-tier read-through cache (in-process L1 LRU + shared **Redis** L2), with the
immutable-block trick (bytes keyed by validator, only the key→validator pointer
invalidated on write), singleflight stampede protection, short negative caching,
and write-around + invalidate. Set `SOS_REDIS_ADDR` to enable the shared tier;
without it the cache is L1-only.

## Configuration (env)

| Var | Default | Notes |
|-----|---------|-------|
| `SOS_LISTEN` | `:9464` | gRPC listen address |
| `SOS_AUTH_TOKEN` | — | shared secret every caller must present (see [Authentication](#authentication)) |
| `SOS_ALLOW_ANONYMOUS` | `false` | accept unauthenticated callers; required when `SOS_AUTH_TOKEN` is unset |
| `SOS_BACKEND` | `minio` | `minio` \| `s3` \| `gcs` \| `azure` \| `mem` |
| `SOS_BUCKET` | — | required (container for Azure) |
| `SOS_REGION` | `us-east-1` | |
| `SOS_ENDPOINT` | — | MinIO / S3-compatible / Azurite endpoint |
| `SOS_ACCESS_KEY` / `SOS_SECRET_KEY` | — | S3 / MinIO credentials |
| `SOS_GCS_CREDENTIALS_FILE` | — | path **inside the container** to a GCS service-account JSON (else ADC) |
| `SOS_AZURE_ACCOUNT` / `SOS_AZURE_KEY` | — | Azure account + shared key |
| `SOS_CACHE` | `true` | enable the cache |
| `SOS_REDIS_ADDR` | — | shared cache tier (empty = L1-only) |
| `SOS_CACHE_MAX_OBJECT_BYTES` | `1048576` | max byte-cached object size |
| `SOS_PROBE_STRATEGY` | `list` | readiness probe: `list` \| `stat` |
| `SOS_PROBE_KEY` | — | required for `stat`; the object headed (need not exist) |
| `SOS_PROBE_INTERVAL` | `10s` | how often readiness is re-probed |
| `SOS_PROBE_TIMEOUT` | `5s` | per-probe deadline |

## Readiness

`Capabilities` is **static feature introspection**: it answers from a table
keyed by backend kind and configuration, reaches no network, and therefore
succeeds against a gateway whose store is unreachable or whose credentials are
refused. Readiness is a separate, live question, answered by a **non-mutating
probe** of the configured bucket/container:

| Strategy | Verb | Requires | Notes |
|----------|------|----------|-------|
| `list` (default) | list one key | list permission on the bucket | detects a missing bucket AND refused credentials |
| `stat` | HEAD `SOS_PROBE_KEY` | read permission on one key | least-privilege fallback; attests reachability only — see below |

A probe never creates or deletes anything, and its failure is normalized to a
cause the operator can act on: `Unavailable` (endpoint unreachable), `NotFound`
(missing bucket/container), `PermissionDenied` (credentials refused).

**`stat` is deliberately the weaker strategy.** It answers over HTTP HEAD, which
carries no error body, so the only thing it can attest is that the endpoint
answered and authenticated the request. Two consequences you must not design
around:

- It does **not** detect a missing bucket/container. A HEAD for an absent key
  and a HEAD against a bucket that does not exist are the same bare 404 on S3,
  MinIO and GCS alike.
- It does **not** detect revoked credentials. S3-compatible stores answer `403`
  rather than `404` for a key that merely does not exist when the caller lacks
  list permission — the exact grant `stat` exists to serve — so `stat` must
  treat `403` as access, and a genuinely revoked grant reads the same way.

Use `list` wherever the grant allows it: it is the only strategy that detects
either condition. Reach for `stat` only when the credentials carry no list
permission at all, and accept that readiness then means "the store answered",
not "the store will serve reads".

Two surfaces expose this:

One background probe every `SOS_PROBE_INTERVAL` (bounded by
`SOS_PROBE_TIMEOUT`) feeds both surfaces, so they never disagree and neither
adds load of its own:

- **`Ready` RPC** — reports `ready`, the normalized `code`, the backend
  `detail`, and `checked_at_unix_ms` so a caller can judge freshness. It answers
  from the latest background probe rather than probing per call: probing per
  call made it an unpaced amplifier onto the cloud API, where a client in a loop
  becomes one LIST per request and the resulting throttling degrades real
  traffic. The agent's `Start` waits on this before reporting the service up.
- **gRPC health service** (`grpc.health.v1`) — the same probe publishes the
  serving status of `codefly.storage.v0.ObjectStorage`.

**Which Kubernetes probe checks what, and why.** The **startup** probe checks
`codefly.storage.v0.ObjectStorage`, so a replica must prove real access to the
bucket before it joins rotation — a bad bucket or credential stalls the rollout
while the previous pods keep serving. **Readiness and liveness check the overall
(`""`) service**, which stays `SERVING` while the process serves.

Readiness deliberately does *not* follow the backend probe. Every replica shares
one bucket and one credential set, so a cloud outage, a deleted bucket or a
revoked grant fails all of them within a single interval; draining on that
empties the Service's endpoint list and converts a degraded gateway — one that
can still serve presigned URLs and cached reads — into an unreachable one.
Access lost after startup is reported per request as `UNAVAILABLE`, which tells
a client more than a connection failure does, and liveness stays put so nothing
is restarted over it.

Cloud backends are qualified per provider: MinIO permissions do not imply S3,
GCS, or Azure parity, so the grant each strategy needs must be verified against
the real backend before a deployment relies on it.

## Authentication

The gateway holds the backend credentials and exposes bucket-wide read, write,
delete, presign and subscribe. Anyone who can reach its port can use all of it,
so **the gateway refuses to start on an unauthenticated listener** unless the
operator says otherwise: `SOS_AUTH_TOKEN` must be set, or `SOS_ALLOW_ANONYMOUS`
must be `true`.

With `SOS_AUTH_TOKEN` set, every RPC — unary and streaming, `Capabilities`
included — must carry the secret as the `x-codefly-token` gRPC metadata header,
the same key the codefly host already uses to authenticate agent plugins.
Anything else is rejected with `UNAUTHENTICATED` before it reaches a backend.
The token is compared in constant time.

Which layer enforces caller identity, per profile:

| Profile | Enforced by | How the token is delivered |
|---------|-------------|----------------------------|
| Local (`codefly run`, tests) | **The gateway.** The agent generates a fresh token every run. | The `object-storage` configuration group, as a `secret`-marked `token` value beside `endpoint` and `connection`. |
| Deployed (Kubernetes) | **The cluster** — NetworkPolicy plus service-mesh mTLS in front of the ClusterIP. The manifest sets `SOS_ALLOW_ANONYMOUS=true` explicitly and says so in a comment. | Not delivered, and gateway-enforced auth is **not available** in this profile. Configuring `SOS_AUTH_TOKEN` for a deployment is rejected by the Builder rather than rendered, because the manifest carries no secret values and consumers would receive no credential. |

The token never appears in the endpoint or connection string, in the startup
log, in the rendered manifest, or in an image layer.

**Migrating an existing deployment.** A gateway that used to start with no
authentication now needs one explicit choice. Set `SOS_AUTH_TOKEN` and have
consumers send the header, or set `SOS_ALLOW_ANONYMOUS=true` to keep the old
behavior and record that some other layer is doing the enforcing. Setting both
is rejected: it is the shape a half-finished migration takes, and picking either
one silently would leave you believing the other is in force. Randomized MinIO
credentials do not satisfy the check — they protect the MinIO port, not this one.

**Turning on gateway-enforced auth for a deployment** is not a configuration
change you can make today, and the Builder says so rather than emitting a broken
manifest. It needs two things that do not exist yet: the Secret channel that
carries a value into the pod's environment (the same one `SOS_SECRET_KEY` and
`SOS_AZURE_KEY` wait on), *and* delivery of that same secret to every consumer's
`object-storage` configuration. Wiring only the first would start a gateway
enforcing a credential none of its consumers hold.

**Limitation.** The token authenticates the caller but the local profile carries
it over plaintext gRPC, so a passive on-path observer on the host's network
could capture it. It is regenerated every run, which bounds the window and
revokes tokens from earlier runs — note that this is the *only* rotation there
is: there is no way to replace a live token in place, so rotating means
restarting the gateway and re-reading configuration in every consumer. A new
token also changes the gateway container's environment, so each run recreates
that container rather than reattaching to the previous one; that is the cost of
the revocation property, not an accident. TLS/mTLS for the local topology is not
implemented here — see
[#3](https://github.com/codefly-dev/service-object-storage/issues/3) for the
general authentication design.

## Running as a codefly service

This repo ships a **codefly service agent** (`codefly.dev/object-storage`) so the
gateway runs as a first-class codefly service — the way `codefly.dev/postgres` or
`codefly.dev/redis` do. A consumer declares it as a `service-dependency` and dials
the `codefly/storage/v0` gRPC endpoint; it never links a cloud SDK.

- **Local / test**: the agent's Runtime starts a **MinIO** container, creates the
  bucket, and runs the gateway container (`SOS_BACKEND=minio`) pointed at it —
  "test on MinIO, ship on S3", decided by config. Both containers publish on all
  interfaces so a consumer container reaches them over the Docker host bridge on
  Linux; each is protected by a per-run credential rather than by the binding —
  a random MinIO root password, and the gateway token described above. Creating
  the bucket proves nothing about the gateway: the agent reaches MinIO on
  localhost with the root credentials, the gateway reaches it over the container
  bridge with its own, so `Start` still waits on the gateway's `Ready` probe.
  The agent probes over the **native** network view, since it is a host process
  even when the service it started runs in a container.
- **Deployed**: the Builder emits a Kubernetes Deployment running the gateway
  image against the configured cloud backend (`SOS_BACKEND` = `s3` | `gcs` |
  `azure`, defaulting to `s3` when the environment names none). `SOS_BUCKET`,
  `SOS_REGION`, and the backend are read from the deployment configuration. GCS
  defaults to keyless auth (Application Default Credentials / Workload Identity);
  a `SOS_GCS_CREDENTIALS_FILE` configured for a deployment is **rejected by the
  Builder**, not rendered: nothing here mounts a service-account key, so emitting
  the path would name a file the pod does not have (see the deployed key-file
  rule below). Azure reads its
  (non-sensitive) account name from `SOS_AZURE_ACCOUNT`, emitted into the
  manifest when the backend is `azure`. (Note: sensitive credential values —
  `SOS_SECRET_KEY`, `SOS_AZURE_KEY` — are not yet wired into the emitted Secret;
  see the credential-delivery follow-up.)

### Cloud credentials in a local run

`SOS_GCS_CREDENTIALS_FILE` is a local-profile setting. The gateway always runs
in a container and only ever reads a container path, which is why the two
profiles treat a configured value differently:

- **Deployed**, the value is **rejected rather than rendered**. Nothing in this
  repo mounts a service-account key, so a path emitted into the manifest would
  name a file the pod does not have; `Builder.Deploy` fails the deploy with an
  actionable message instead. Deployed GCS authenticates keylessly — Workload
  Identity on GKE. Off GKE (EKS, AKS, on-prem) there is no Workload Identity and
  no metadata server: mount the key from an overlay this agent does not own and
  patch `SOS_GCS_CREDENTIALS_FILE` onto the container there.
- **Locally**, the value is a path on your machine, absolute or relative to the
  service directory (never to whatever directory the agent happens to run from).
  The Runtime validates it, copies it into an invocation-owned directory under
  `$CODEFLY_HOME`, mounts *that file only* at
  `/codefly/credentials/gcs-service-account.json`, and sets
  `SOS_GCS_CREDENTIALS_FILE` to that container path. The copy is mode `0444` in a
  `0700` directory, and the gateway container is pinned to the unprivileged user
  `65532:65532`: it can read the copy and — owning neither it nor root — cannot
  write to it, and your own key file is never exposed to the container.
  `codefly destroy` removes the copy, and so does a run that fails part-way.

  The copy is named after a digest of the key, so rotating it on the host —
  including an atomic replace — changes the mount and the gateway container is
  recreated on the next run rather than continuing to serve the superseded key.

Two local modes are deliberately **not** supported, and fail or stay unavailable
rather than half-working:

- **GCS without a key file.** The container inherits no host identity, so
  Application Default Credentials and Workload Identity are deployment-only.
  A local `gcs` run without `SOS_GCS_CREDENTIALS_FILE` fails at `Init` telling
  you to set one.
- **Azure.** `SOS_AZURE_ACCOUNT` is forwarded (it is a public name, not a
  credential), but the shared key has no local delivery path yet, so a local
  `azure` run leaves the gateway unable to authenticate *to Azure Blob storage*
  — distinct from the caller-facing token above, which is always enforced. Use
  the MinIO default or `s3`/`gcs` locally.

S3 and MinIO take their credentials as `SOS_ACCESS_KEY` / `SOS_SECRET_KEY`, which
are values rather than paths and need no host-versus-container distinction.

The agent files live at the repo root (`agent.codefly.yaml`, `main.go`,
`runtime.go`, `builder.go`, `templates/`); the gateway itself is unchanged and
still builds from `cmd/service-object-storage` (see `Dockerfile`). The agent
binary is the release asset that makes `codefly.dev/object-storage` resolvable;
the gateway ships as the `ghcr.io/codefly-dev/service-object-storage` image.

## Develop

```bash
# regenerate stubs from proto (requires buf + protoc-gen-go/-grpc)
buf generate

go build ./...
go vet ./...
go test ./...            # unit tests (mem backend + miniredis)

# integration against real MinIO
docker run -d --name m -p 9000:9000 quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z server /data
MINIO_ENDPOINT=127.0.0.1:9000 MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin MINIO_BUCKET=sos-test \
  go test -tags integration ./internal/integration/...
docker rm -f m
```

## Layout

```
proto/codefly/storage/v0/    the uniform API
gen/                         generated gRPC stubs
internal/backend/            Backend interface + s3, gcs, azure, minio, mem
internal/cache/              two-tier read-through cache
internal/auth/               caller authentication on the gRPC surface
internal/server/             gRPC ObjectStorage implementation
internal/config/             env configuration
cmd/service-object-storage/  the server binary
```
