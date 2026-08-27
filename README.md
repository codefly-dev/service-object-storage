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
| `SOS_BACKEND` | `minio` | `minio` \| `s3` \| `gcs` \| `azure` \| `mem` |
| `SOS_BUCKET` | — | required (container for Azure) |
| `SOS_REGION` | `us-east-1` | |
| `SOS_ENDPOINT` | — | MinIO / S3-compatible / Azurite endpoint |
| `SOS_ACCESS_KEY` / `SOS_SECRET_KEY` | — | S3 / MinIO credentials |
| `SOS_GCS_CREDENTIALS_FILE` | — | GCS service-account JSON (else ADC) |
| `SOS_AZURE_ACCOUNT` / `SOS_AZURE_KEY` | — | Azure account + shared key |
| `SOS_CACHE` | `true` | enable the cache |
| `SOS_REDIS_ADDR` | — | shared cache tier (empty = L1-only) |
| `SOS_CACHE_MAX_OBJECT_BYTES` | `1048576` | max byte-cached object size |

## Running as a codefly service

This repo ships a **codefly service agent** (`codefly.dev/object-storage`) so the
gateway runs as a first-class codefly service — the way `codefly.dev/postgres` or
`codefly.dev/redis` do. A consumer declares it as a `service-dependency` and dials
the `codefly/storage/v0` gRPC endpoint; it never links a cloud SDK.

- **Local / test**: the agent's Runtime starts a **MinIO** container, creates the
  bucket, and runs the gateway container (`SOS_BACKEND=minio`) pointed at it —
  "test on MinIO, ship on S3", decided by config.
- **Deployed**: the Builder emits a Kubernetes Deployment running the gateway
  image against the configured cloud backend (`SOS_BACKEND` = `s3` | `gcs` |
  `azure`, defaulting to `s3`) with credentials from a Secret. S3 and Azure read
  their credential values from the Secret via `envFrom` (`SOS_ACCESS_KEY` /
  `SOS_SECRET_KEY`, `SOS_AZURE_ACCOUNT` / `SOS_AZURE_KEY`); GCS mounts the
  Secret's `SOS_GCS_CREDENTIALS_JSON` key as a file and points
  `SOS_GCS_CREDENTIALS_FILE` at it.

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
internal/server/             gRPC ObjectStorage implementation
internal/config/             env configuration
cmd/service-object-storage/  the server binary
```
