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
| `Get` | streaming read; range + conditional |
| `GetMany` | several objects in one streamed call, in request order, each at its listed ETag |
| `Put` | streaming write; multipart hidden in the backend; conditional (create-if-absent / CAS) |
| `Delete` / `DeleteMany` | delete; batch with per-key results |
| `List` | prefix + delimiter + opaque page token |
| `Copy` | server-side copy where supported |
| `Presign` | time-limited URL the client uses directly over plain HTTP (no cloud SDK) |
| `Watch` | stream of write events (Put/Delete) served by the replica the client is connected to |
| `Capabilities` | machine-readable feature set — introspect before calling |
| `Ready` | probe the backing store: is the bucket reachable with these credentials? |
| `Native` | escape hatch for backend-specific verbs |

Errors are normalized to gRPC status codes (`NotFound`, `AlreadyExists`,
`FailedPrecondition`, `Unimplemented`, …) so clients never parse an S3 XML code.

`Stat`, `Get`, `GetMany`, `List` and `Capabilities` can also read a bucket the caller
names, with the caller's credential — see
[bound calls](#reading-a-callers-own-bucket-bound-calls).

## Reading a caller's own bucket (bound calls)

`Stat`, `Get`, `GetMany`, `List` and `Capabilities` take an optional `Binding`: kind
(`s3`, `minio`, `gcs`, `azure`), endpoint, region, bucket, prefix, and the
credential for that one call. A request that carries a binding runs against the
bucket it names and never touches the configured one; a request without one is
unchanged. It is how a host reads a tenant's own bucket — its datasource — over
every backend this gateway has, without a store client of its own.

```text
ListRequest { prefix: "", limit: 0,
  binding: { kind: "s3", endpoint: "https://s3.eu-west-1.amazonaws.com",
             region: "eu-west-1", bucket: "acme-docs", prefix: "kb/",
             credential: { access_key: { id: "AKIA…", secret: "…" } } } }
```

- **The binding is an argument, not a configuration.** The gateway opens a
  client for the call and closes it, and its connections, when the call
  returns. It does not store, log or return the credential, and the credential
  fields are marked `debug_redact`. The caller decrypts the credential for the
  call, as it would for an in-process client.
- **Only the binding's credential is used.** `s3`/`minio` take an access key,
  `azure` takes the account name and shared key in the same `access_key`, and
  `gcs` takes a service-account JSON key. Google keys of any other type are
  refused, because an external-account key names a local file or command as
  its token source. A missing credential is `INVALID_ARGUMENT`. The call never
  falls back to the gateway's own identity, which would lend the caller the
  gateway's grants.
- **The public-destination guard.** A binding's endpoint is supplied by the
  caller, so it is an egress the gateway must not let reach its own network.
  Every dial a bound call makes, including the token exchange of a GCS key, is
  judged against the concrete IP being connected to. That covers the first
  request and every redirect, so neither DNS rebinding nor a redirect gets
  past it. Any address outside the public internet is refused: loopback,
  RFC 1918, link-local (cloud metadata included), CGNAT, documentation,
  benchmarking, reserved, NAT64, 6to4 and Teredo, read from the IANA
  special-purpose registries. The client never uses a proxy. A named endpoint
  is also resolved and judged before the first dial, so the refusal is
  immediate and names it:
  `PERMISSION_DENIED: binding endpoint "localhost": destination is not a public address`.
  The endpoint must be a bare `http(s)` origin with no path, query or userinfo.
  If it is empty, the kind's public service is used. S3-style stores are
  addressed path-style.
- **Caps.** A bound `Get` streams at most 5 MiB per object; a larger object is
  refused with `RESOURCE_EXHAUSTED`, and so is a store that sends more than the
  size it declared. A bound `List` returns at most 1,000 objects per page, and
  a zero or larger `limit` is clamped to that; page with `next_page_token` for
  more. `Capabilities` with the same binding reports both caps
  (`max_object_bytes`, `max_list_limit`), the bound kind and whether it has
  `versions`. It reports only the read surface, because a bound call cannot
  write or presign.
- Keys are relative to the binding's `prefix`, exactly as `SOS_PREFIX` works
  for the configured bucket.
- **Batched reads.** `GetMany` takes up to `max_get_many_items` (1,000) keys,
  each with the ETag the caller listed as `if_match`, and streams them in
  request order in one call. Each item gets a header with its `index` and an
  outcome, followed by data chunks totalling exactly `info.size` when the
  outcome is `OK`.
  - `CHANGED` means the ETag no longer matches, `NOT_FOUND` means the key is
    gone, and `TOO_LARGE` means the object is over the bound cap. None of
    these is followed by data.
  - Any failure that isn't about one item (refused credential, unreachable
    store, guard refusal) ends the stream with its status.
  - A bound batch opens one guarded client for all its items. It also works
    without a binding, against the configured bucket.

### The guard's one exception: composed dependencies

A binding may name a non-public endpoint in exactly one case: it is the
endpoint of a service this gateway's service declares as a
`service-dependency`, as codefly resolved it. The agent's Runtime reads the
dependency network mappings codefly hands it, takes each one's container
address (the gateway runs in a container), and delivers them to the gateway as
`SOS_COMPOSED_ENDPOINTS` (`host:port`, comma-separated). The admission is exact:

- the binding's endpoint must match a composed endpoint by host **and** port —
  the same address under another name, or another port on the same host, is
  refused;
- the gateway then dials only the addresses that name resolves to, on that
  port — a redirect anywhere else is judged by the public-only rule and refused;
- every other loopback, private, link-local or reserved destination is refused
  as before, and a public endpoint is admitted as before.

There is no switch that widens this: no "allow private" setting, no flag that
disables the guard, no local-versus-hosted mode. A service with no dependency
admits nothing beyond the public internet, and that is every environment's rule
— a deployed gateway is given no composed endpoints today, so it binds only
public stores. `SOS_COMPOSED_ENDPOINTS` is the Runtime's output, not an
operator setting.

The MinIO a local run starts as this gateway's own backend is not a dependency
and stays unbindable.

## Byte path

- **Small objects** are proxied through the gateway.
- **Large objects** use `Presign` — the client transfers directly to/from the
  backend over plain HTTP (still no cloud SDK). Presign is GET-biased: writes
  should route through `Put` so they produce a `Watch` event.

## Watch

`Watch` streams a change hint for every `Put`/`Delete` that completes through
the gateway, scoped by key prefix. Delivery is **per replica**: a stream carries
the writes served by the replica the client is connected to, never writes served
by other replicas or made to the bucket directly. It is a hint stream, not a
log — a consumer reconciles against `List` periodically and after a
`RESOURCE_EXHAUSTED` drop (see the `WriteEvent` contract in the proto). A
consumer that needs every write to the bucket across replicas subscribes to the
backing store's event notifications (S3/MinIO bucket notifications, GCS Pub/Sub
notifications, Azure Event Grid) instead.

## Configuration (env)

| Var | Default | Notes |
|-----|---------|-------|
| `SOS_LISTEN` | `:9464` | gRPC listen address |
| `SOS_AUTH_TOKEN` | — | shared secret every caller must present (see [Authentication](#authentication)) |
| `SOS_ALLOW_ANONYMOUS` | `false` | accept unauthenticated callers; required when `SOS_AUTH_TOKEN` is unset |
| `SOS_BACKEND` | `minio` | `minio` \| `s3` \| `gcs` \| `azure` \| `mem` |
| `SOS_BUCKET` | — | required (container for Azure) |
| `SOS_PREFIX` | — | confine the gateway to one key prefix inside the bucket; keys are relative to it |
| `SOS_REGION` | `us-east-1` | |
| `SOS_ENDPOINT` | — | MinIO / S3-compatible / Azurite endpoint; **required** for `minio` |
| `SOS_ACCESS_KEY` / `SOS_SECRET_KEY` | — | S3 / MinIO credentials |
| `SOS_GCS_CREDENTIALS_FILE` | — | path **inside the container** to a GCS service-account JSON (else ADC) |
| `SOS_AZURE_ACCOUNT` / `SOS_AZURE_KEY` | — | Azure account + shared key |
| `SOS_PROBE_STRATEGY` | `list` | readiness probe: `list` \| `stat` |
| `SOS_PROBE_KEY` | — | required for `stat`; the object headed (need not exist) |
| `SOS_PROBE_INTERVAL` | `10s` | how often readiness is re-probed |
| `SOS_PROBE_TIMEOUT` | `5s` | per-probe deadline |
| `SOS_COMPOSED_ENDPOINTS` | — | written by the agent's Runtime, not an operator: the composed dependencies a binding may name although not public (see [the guard's exception](#the-guards-one-exception-composed-dependencies)) |

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
can still serve presigned URLs — into an unreachable one.
Access lost after startup is reported per request as `UNAVAILABLE`, which tells
a client more than a connection failure does, and liveness stays put so nothing
is restarted over it.

Cloud backends are qualified per provider: MinIO permissions do not imply S3,
GCS, or Azure parity, so the grant each strategy needs must be verified against
the real backend before a deployment relies on it.

## Durable local MinIO

Local MinIO uses an explicitly owned data directory that survives container
replacement, with custody checks before recreation. A new store initializes
on its first run with no opt-in when its custody directory is absent or empty —
there is nothing on disk to lose, which is also what a CI run in a throwaway
Codefly home sees. A directory that holds anything but has no custody record, a
live MinIO container without one, and existing anonymous-volume stores fail
closed and require recovery. `SOS_LOCAL_MINIO_INITIALIZE` is no longer read.
See [local custody and retained-volume recovery](docs/local-minio-recovery.md)
for ownership, paths, lifecycle behavior and recovery instructions.

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
Core's `callertoken` package compares the token in constant time.
Only the exact `/grpc.health.v1.Health/Check` and
`/grpc.health.v1.Health/Watch` RPCs are exempt; `Health/List` requires a token.
Generated tokens are 64 lowercase hex characters (32 random bytes).
Operator-provided tokens retain their configured value. The same generator
supplies local MinIO passwords and custody identifiers.

An empty token passed to a core interceptor fails closed. The gateway installs
interceptors only when `SOS_AUTH_TOKEN` is nonempty: the explicit anonymous
profile continues to install none. Core's `CheckListener` preserves the refusal
of both or neither auth settings; its errors name the configuration and exposure,
while deployment and migration guidance lives here.

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
gateway runs as a first-class codefly service — the way `codefly.dev/postgres`
does. A consumer declares it as a `service-dependency` and dials
the `codefly/storage/v0` gRPC endpoint; it never links a cloud SDK.

- **Local / test**: the agent's Runtime starts a **MinIO** container with persistent
  custody (explicit bootstrap creates the initial bucket), and runs the gateway container (`SOS_BACKEND=minio`) pointed at it —
  "test on MinIO, ship on S3", decided by config. The two containers share a
  Docker network of their own (`codefly-<service>-network`, scoped to the
  workspace like the containers, removed with them), and the gateway dials MinIO
  there as `http://minio:9000` — never through a published port and
  `host.docker.internal`, which a rootless daemon or a host firewall can refuse.
  MinIO's published port is bound to 127.0.0.1 for the agent alone; the gateway
  publishes on all interfaces so a consumer container reaches it over the Docker
  host bridge on Linux. Each is protected by a per-run credential rather than by
  the binding — a random MinIO root password, and the gateway token described
  above. Because presigning is computed by the gateway against the endpoint it
  dials, a locally presigned URL names `minio:9000`: only a container on the
  service network can fetch it. Creating the bucket proves nothing about the
  gateway: the agent reaches MinIO on localhost with the root credentials, the
  gateway reaches it over the service network with its own, so `Start` still
  waits on the gateway's `Ready` probe.
  The agent probes over the **native** network view, since it is a host process
  even when the service it started runs in a container.
- **Deployed**: the Builder emits a Kubernetes Deployment running the gateway
  image against the configured cloud backend (`SOS_BACKEND` = `s3` | `gcs` |
  `azure`, defaulting to `s3` when the environment names none). `SOS_BUCKET`,
  `SOS_PREFIX`, `SOS_REGION`, `SOS_ENDPOINT` and the backend are read from the
  deployment configuration (the `object-storage` group), overriding the service
  spec. The container also declares `CODEFLY__SERVICE=<service name>`, so an
  environment's service configuration bound by the CLI after the render replaces
  those `SOS_*` literals by name — the usual way a cell selects its store over a
  spec still on the local default (`backend: minio`). A deployment never starts
  MinIO, so a gateway left on `minio` without `SOS_ENDPOINT` **refuses to start**
  naming the missing variable and the remedy (the render cannot refuse it: the
  binding it would need to see happens afterwards). The Service publishes the
  in-cluster port core allocated to the gRPC endpoint (falling back to 9464) and
  forwards it to the container's 9464. The pod runs under its own Kubernetes ServiceAccount, named after the service
  and rendered in the target namespace (never the namespace `default`), so a
  keyless store can grant exactly this workload — on GKE the bucket IAM member
  `serviceAccount:<project>.svc.id.goog[<namespace>/<service name>]`. GCS
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

The agent runs that image by digest, not by tag: `gateway-image.json` records
the digest and the agent pins it, so the container a release runs — and reports
evidence for — is the one that was built for it rather than whatever the tag
serves now. The digest only exists once the image is built, so the image is
published ahead of the release tag by `publish-gateway-image.yml`, with the
upcoming stable version.

That workflow builds nothing itself. It is a job in this repository holding
`packages: write`, whose one substantive step is core's
`.github/actions/publish-service-image`, pinned by commit SHA. The split is
GHCR's: it authorizes the repository that *defines* the job, so a job defined
in core could not write this repository's package — which is why core ships a
composite action to call rather than a reusable workflow to delegate to. The
build recipe, the version check, the digest and the lock are therefore one
implementation shared with every codefly service gateway, while the credential
and the dispatch stay here.

Before it logs in or builds, the action refuses anything but a
`workflow_dispatch`, a commit not reachable from this repository's own remote
default branch, a `HEAD` that is not the triggering commit, and a build context
that is not the clean committed tree — any modified, staged, untracked or
ignored file. Its checkout is the caller's: the triggering commit with no `ref`
override, full history, and `persist-credentials: false`, since a persisted
credential would otherwise reach the Docker build context.

It pushes by digest with no tag, stamps
`org.opencontainers.image.version` on every platform, and uploads the two-field
lock as the artifact `gateway-image.json-<version>`. Adopt that artifact as
`gateway-image.json` in a reviewed PR before the coordinator runs
`codefly publish`; never hand-tag. `release.yml` creates the `:<version>` and
`:latest` image tags from that digest and refuses one that was not built for the
version being released.

Core's `resources.ParseImageLock` reads the gateway lock. It requires exactly
`ghcr.io/codefly-dev/service-object-storage` and `sha256:` followed by 64
lowercase hexadecimal characters, refuses unknown fields (including `tag`) and
trailing JSON documents, and reports an empty digest as `ErrImageUnpublished`.
MinIO retains its separate source/release/revision lock schema and uses core's
same lowercase SHA-256 validation.

## Supply chain evidence

`Builder.SBOM` answers both scopes of the shared Codefly SBOM contract.

- **Source scope** — the default, and what an unspecified scope means — returns
  the package inventory this agent has always produced.
- **Image scope** returns CycloneDX evidence bound to the immutable digest and
  platform of each image the service ships. A caller that already resolved the
  images passes them as subjects; an empty subject list asks the agent to
  enumerate its own.

The gateway is the only image this service ships, and it is published for
`linux/amd64` and `linux/arm64`. Each platform is its own subject, so evidence
for one never stands in for the other. MinIO backs local runs and never reaches
a deployment manifest, so it is deliberately not inventoried: reporting it would
claim coverage of something the deployed service does not run.

`SOS_GATEWAY_IMAGE` is honored. An override is a local build no registry serves,
so it is inventoried through the Docker daemon — which needs `syft` on PATH —
and covers the single platform the daemon holds. Its subject carries the image
ID and actual platform the daemon resolved the override to. Core scans that
immutable ID even if a local tag moves after inspection; caller-declared local
platforms are refused. An override whose reference the daemon does not hold is
refused rather than inventoried under an identity nothing named.

A failed scan, an unresolvable image, or a digest other than the one requested is
reported as an error, never as partial or complete coverage.

`cmd/image-sbom` writes that evidence to disk through the same scanner the agent
uses: core's `ImageSubjects` derives registry subjects through
`ExpectedFromImageReference` (which refuses bare tags), and
`CollectImageEvidence` / `WriteImageEvidenceIndex` collect and index them.
The Builder and command both validate explicit expected digests through core's
`ImageForSubject` path:

```bash
# the published multi-architecture image: one document per shipped platform
go run ./cmd/image-sbom -image "$(jq -er '.name + "@" + .digest' gateway-image.json)" -out sbom

# a locally built image, which no registry serves (needs syft on PATH)
docker build -t service-object-storage:e2e .
go run ./cmd/image-sbom -image service-object-storage:e2e -local -out sbom
```

Each document carries the digest it was scanned from in its own root component,
so it still names the image it describes once separated from the directory it
was written in. Every release runs the same command and publishes the documents
and an `index.txt` naming each digest and platform, as build artifacts of
`.github/workflows/release.yml`. The platform list lives only in
`internal/imageevidence`, which now contains only the service declaration and
calls core. A test asserts the platforms against the publisher's build step.
Document filenames include repository, platform and digest; each index
row contains platform (`-` if unstated), repository plus scanned digest, filename
and canonical inventory checksum (not the checksum of the formatted file).

## Develop

```bash
# regenerate stubs from proto (runs buf in codefly's pinned proto companion image)
codefly generate proto --proto proto --output . --template buf.gen.yaml

go build ./...
go vet ./...
go test ./...            # unit tests (mem backend)

# integration against real MinIO
docker run -d --name m -p 9000:9000 ghcr.io/codefly-dev/minio@sha256:6db9ae5fd307001ad5bb1899a8a4b8f69aebe982f95b7085416953b5f4cc22a5 server /data
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
internal/events/             in-process write-event hub behind Watch
internal/server/             gRPC ObjectStorage implementation (binding.go: bound calls)
internal/egress/             public-destination guard for bound calls
internal/config/             env configuration
cmd/service-object-storage/  the server binary
```
