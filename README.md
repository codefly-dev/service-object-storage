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
| `SOS_GCS_CREDENTIALS_FILE` | — | path the gateway reads a GCS service-account JSON from (else ADC) |
| `SOS_AZURE_ACCOUNT` / `SOS_AZURE_KEY` | — | Azure account + shared key |
| `SOS_CACHE` | `true` | enable the cache |
| `SOS_REDIS_ADDR` | — | shared cache tier (empty = L1-only) |
| `SOS_CACHE_MAX_OBJECT_BYTES` | `1048576` | max byte-cached object size |

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
  a random MinIO root password, and the gateway token described above.
- **Deployed**: the Builder emits a Kubernetes Deployment running the gateway
  image against the configured cloud backend (`SOS_BACKEND` = `s3` | `gcs` |
  `azure`, defaulting to `s3` when the environment names none). `SOS_BUCKET`,
  `SOS_REGION`, and the backend are read from the deployment configuration. GCS
  authenticates keylessly, via Application Default Credentials / Workload
  Identity — bind the workload's Kubernetes ServiceAccount to a GCP service
  account and no key file is needed. A `SOS_GCS_CREDENTIALS_FILE` configured for
  a deployment is **rejected by the Builder**, not rendered: nothing here mounts
  a service-account key, so emitting the path would name a file the pod does not
  have. It remains a local-profile setting. Azure reads its (non-sensitive)
  account name from `SOS_AZURE_ACCOUNT`, emitted into the manifest when the
  backend is `azure`. (Note: sensitive credential values — `SOS_SECRET_KEY`,
  `SOS_AZURE_KEY` — are not yet wired into the emitted Secret; see the
  credential-delivery follow-up.)

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
docker run -d --name m -p 9000:9000 minio/minio server /data
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
