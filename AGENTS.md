# service-object-storage

A gRPC gateway serving a uniform object-storage API (`codefly/storage/v0`) over
S3 / GCS / Azure Blob / MinIO, plus the codefly service agent
(`codefly.dev/object-storage`) that runs it. Clients speak the gRPC contract and
never link a cloud SDK.

`README.md` is the reference for the API, the cache, readiness, authentication
and the local/deployed profiles. This file is how to work in the repo.

## What this repo owns

- The contract and its stubs: `proto/codefly/storage/v0`, `gen/`.
- The gateway server: `cmd/service-object-storage` and `internal/**` — the
  backends, the two-tier cache, caller auth, the readiness probe, and the
  normalization of backend errors to gRPC codes.
- The codefly agent that runs the gateway as a first-class service: the repo
  root (`main.go`, `runtime.go`, `builder.go`, `agent.codefly.yaml`) and
  `templates/`, the Kubernetes manifests the Builder renders.
- SBOM evidence for the image it ships: `cmd/image-sbom`,
  `internal/imageevidence`.

## What it does not own

- The CLI and the agent framework — `codefly-dev/cli`, and `codefly-dev/core`,
  which is both a `go.mod` dependency and the home of the shared CI workflows
  this repo calls (pinned here by commit SHA, never `@main`).
- The backing stores. Behaviour differences between MinIO, S3, GCS and Azure
  are absorbed in `internal/backend/`, but a broken store is not a bug here.
- Consumers. They declare a `service-dependency` and dial the endpoint; nothing
  about them is configured in this repo.

## Build and test

These are what CI runs — `.github/workflows/ci.yml` plus
`codefly-dev/core/.github/workflows/go-service-ci.yml` at the SHA pinned there.
Run them from the repo root; `go.mod` is at the root and there is no nested
module.

```bash
go build ./...
go vet ./...
go mod tidy -diff      # CI fails on an untidy go.mod/go.sum
go test ./...          # CI runs this with -v
go test -race ./...    # its own CI job: cache, events hub and Watch are concurrent
```

Integration against a real MinIO, behind the `integration` build tag:

```bash
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e server /data
MINIO_ENDPOINT=127.0.0.1:9000 MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin MINIO_BUCKET=sos-test \
  go test -tags integration -count=1 ./internal/integration/...
```

quay.io by digest is not incidental: the Docker Hub `minio/minio` repository no
longer serves anonymous pulls, and this digest is the image the agent's Runtime
itself pins.

End-to-end through the agent Runtime and the image-SBOM path (`e2e` tag) needs a
locally built gateway image; the SBOM test additionally needs `syft` on PATH:

```bash
docker build -t service-object-storage:e2e .
SOS_GATEWAY_IMAGE=service-object-storage:e2e \
  go test -tags e2e -count=1 -timeout 600s .
```

**A green integration run is not evidence that anything ran.** With
`MINIO_ENDPOINT` unset, all three tests call `t.Skip`, the package still prints
`ok`, and `go test` exits **0**. Read `-v` output for `--- PASS`, never `ok`
alone. Forgetting the tag is the honest failure instead: `matched no packages`,
exit 1. The `e2e` tag is mixed — without `SOS_GATEWAY_IMAGE` the custody tests
fail loudly while the Runtime and SBOM tests skip.

`.github/workflows/agent-ci.yml`, the full agent conformance gate, runs only on
`workflow_dispatch`; the file records why and what must land upstream first. It
is not a PR gate, so its silence on a PR is not a pass.

## Releases

A `v*` tag publishes the gateway image first and the agent binary second, from
one workflow, in that order. The tag must match `version:` in
`agent.codefly.yaml` or the job fails by design: an agent published ahead of
that file resolves the *previous* image, which exists, so nothing fails at run
time and the evidence describes an image the agent never runs. The shipped
platform list lives in `internal/imageevidence` and a test asserts it against
the release workflow.

## How to behave when something does not work

1. **Never hack. Give the best fix, even when it spans repos.** A fix that
   belongs in `codefly-dev/cli` or `codefly-dev/core` is not a reason to work
   around it here. `agent-ci.yml` is the worked example: the CLI's pinned
   source-packager predates this repo's Go 1.27 toolchain, so the workflow is
   parked on `workflow_dispatch` naming the upstream issue and the order to
   land it — rather than downgrading the toolchain to make a gate go green.
2. **A gap in the tooling is a bug in the tooling.** When the capability you
   need does not exist, file it where it belongs and say so. Do not hand-build
   what the missing tool was supposed to produce.
3. **Never hardcode what the system resolves** — injected environment, derived
   ports, endpoints, or a credential copied out of another component. This repo
   is built the other way round: the agent mints a fresh gateway token per run
   (unless an operator pinned one) and delivers it through the `object-storage`
   configuration group, and a local GCS key is copied into an invocation-owned
   directory under `$CODEFLY_HOME` and mounted at a fixed container path, never
   the host path. Where a value has no delivery path, the Builder **rejects**
   it rather than rendering it — `SOS_AUTH_TOKEN` and `SOS_GCS_CREDENTIALS_FILE`
   for a deployment. Respect that refusal; do not paper over it.
4. **Diagnose rather than pattern-match.** Put the value back and confirm it
   breaks before believing you found the cause, and do not trust an error
   message before checking it. Do not trust a green either — see the skipped
   integration suite above.
5. **Classify every change that makes something work, in the PR body, as a fix
   or a hack.** A hack does not become a fix by working. If it is a hack, name
   the real fix and where it lives.
6. **Say what you did not verify.** Name the suites you did not run and the
   backends you could not reach. Local work reaches MinIO only; S3, GCS and
   Azure parity is not observable from here, and "MinIO passed" is never "S3
   works".

## Where the depth is

- `README.md` — API, byte path, cache, readiness semantics (including why
  `stat` is the weaker probe), authentication, local vs deployed profiles,
  supply-chain evidence.
- `docs/local-minio-recovery.md` — local MinIO custody, retained volumes and
  recovery.
- `.claude/skills/local-test-suites/` — running the integration and e2e suites
  against real containers, and proving they actually ran.
