# service-object-storage

A gRPC gateway serving a uniform object-storage API (`codefly/storage/v0`) over
S3 / GCS / Azure Blob / MinIO, plus the codefly service agent
(`codefly.dev/object-storage`) that runs it. Clients speak the gRPC contract and
never link a cloud SDK.

`README.md` is the reference for the API, Watch, readiness, authentication
and the local/deployed profiles. This file is how to work in the repo.

## What this repo owns

- The contract and its stubs: `proto/codefly/storage/v0`, `gen/`.
- The gateway server: `cmd/service-object-storage` and `internal/**` — the
  backends, the per-replica Watch event hub, auth configuration, the readiness probe,
  and the normalization of backend errors to gRPC codes.
- The codefly agent that runs the gateway as a first-class service: the repo
  root (`main.go`, `runtime.go`, `builder.go`, `agent.codefly.yaml`) and
  `templates/`, the Kubernetes manifests the Builder renders.
- The evidence command and image declaration: `cmd/image-sbom` and
  `internal/imageevidence`. Core owns subject derivation, scanning and indexing.

## What it does not own

- The CLI and the agent framework — `codefly-dev/cli`, and `codefly-dev/core`,
  which is both a `go.mod` dependency and the home of the shared CI workflows
  this repo calls (pinned here by commit SHA, never `@main`). Core also
  owns `callertoken`, the gateway image-lock parser and shared image evidence.
  Do not restore service-local copies. Pin dependencies with
  `codefly agent deps --pin <version>` so every owned lock is checked.
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
go test -race ./...    # its own CI job: events hub, Watch and readiness monitor are concurrent
```

Integration against a real MinIO, behind the `integration` build tag:

```bash
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  ghcr.io/codefly-dev/minio@sha256:6db9ae5fd307001ad5bb1899a8a4b8f69aebe982f95b7085416953b5f4cc22a5 server /data
MINIO_ENDPOINT=127.0.0.1:9000 MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin MINIO_BUCKET=sos-test \
  go test -tags integration -count=1 ./internal/integration/...
```

The gcs backend's integration tests in the same package run against
fake-gcs-server when `STORAGE_EMULATOR_HOST` (e.g. `localhost:4443`, matching its `-public-host`) and
`GCS_BUCKET` are set, and skip otherwise; the skill below has the command. The
emulator accepts any caller, so it proves the keyless client path and
`SOS_PREFIX`, never Workload Identity or a bucket IAM grant.

The MinIO image is built here, from source, not pulled from MinIO. Docker Hub
`minio/minio` stopped serving anonymous pulls first; on 2026-09-24
`quay.io/minio/minio` and `quay.io/minio/mc` followed (401 even for a manifest
read), which stopped every run that starts MinIO. `images/minio/Dockerfile`
builds the pinned upstream release (RELEASE.2025-04-22T22-12-26Z, commit
`0d7408fc`) unmodified from MinIO's public AGPL-3.0 source, reproducibly, and
`publish-minio-image.yml` pushes it for linux/amd64 and linux/arm64 to the
public `ghcr.io/codefly-dev/minio`, labelled with the exact upstream source.
`minio-image.json` records the digest; the agent embeds it (`minioImage`), and
CI, this file, README.md and the local-test-suites skill restate it —
`TestMinioImageIsTheOneTheLockRecords` fails when any of them disagree or name
quay.io again.

To move MinIO: change `MINIO_RELEASE_TAG`/`MINIO_COMMIT` in the Dockerfile and
the workflow together, merge, take the digest the workflow run prints (its
`minio-image-lock` artifact) into `minio-image.json` and every restatement, and
re-verify custody: the agent keeps `.codefly-custody.json` in the drive root
MinIO serves, so a release that prunes unknown root entries breaks it.

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
`workflow_dispatch`. The upstream fix it waited on has landed and it passes, but
making it a PR gate is a separate decision. Until then its silence on a PR is
not a pass.

## Releases

A `v*` tag publishes the gateway image first and the agent binary second, from
one workflow, in that order. The tag must match `version:` in
`agent.codefly.yaml` or the job fails by design: an agent published ahead of
that file resolves the *previous* image, which exists, so nothing fails at run
time and the evidence describes an image the agent never runs. The shipped
platform list lives in `internal/imageevidence` and a test asserts it against
the `platforms` that `publish-gateway-image.yml` passes to core's publish
action. Core's agent-release workflow accepts only pushed tags whose commits
are reachable from the default branch.

Prepare the gateway before publishing the agent: use `codefly publish --dry-run`
to determine the next version, then dispatch `publish-gateway-image.yml` with
that explicit `version` (without `v`). Adopt its `gateway-image.json-<version>`
artifact as `gateway-image.json` in a reviewed PR. Only then run normal
`codefly publish`; the tag gate still checks the image label against the bumped
agent version and refuses a stale lock. It changes no release tag or lock in
this repository.

The publishing work is core's `.github/actions/publish-service-image`, called
as a step and pinned by commit SHA. The *job* stays here, with its
`packages: write` and its `workflow_dispatch` input, because GHCR authorizes
the repository that defines the job — core's reusable workflow was denied write
access to this repository's package, and that reusable publisher no longer
exists upstream. The action checks out nothing itself, so this repo's checkout
has to satisfy it: the triggering commit (no `ref`), full history,
`persist-credentials: false`, and no step between the checkout and the action.
It then refuses a non-dispatch event, a commit not reachable from this
repository's own remote default branch, a `HEAD` other than the triggering SHA,
and any modified, staged, untracked or ignored file — the Docker context must be
the committed tree, so generated code or a version stamp has to be committed
first. Those refusals run before the registry login or the build. Do not add a
local build, login, push or upload step back: a second publishing path without
those guards is the one that gets used in a hurry, and
`image_release_preparation_test.go` fails when one appears.

The gateway lock accepts only its expected registry/name, a lowercase SHA-256
digest, and the two known JSON keys; an empty digest means unpublished. MinIO's
source metadata has its own schema but shares core's digest validation. Registry
evidence rejects bare tags and declares one subject per shipped platform; local
evidence reads its image ID and actual platform from Docker.

Caller tokens are 64 hex characters, also used for generated MinIO passwords
and custody IDs. Only exact health Check/Watch RPCs bypass the token; Health/List
requires it. An empty interceptor token fails closed, while the gateway's
explicit anonymous profile continues to install no interceptors.

For an agent checkout, run `codefly agent ci` for source, release builds and
fresh generated-service conformance. `codefly ci run` needs a service workspace
and reports `no workspace found` at this repository root.

## How to behave when something does not work

1. **Never hack. Give the best fix, even when it spans repos.** A fix that
   belongs in `codefly-dev/cli` or `codefly-dev/core` is not a reason to work
   around it here. `agent-ci.yml` is the worked example: when the CLI's pinned
   source-packager predated this repo's Go 1.27 toolchain, the workflow was
   parked on `workflow_dispatch` naming the upstream issue (codefly-dev/cli#499)
   and the order to land it — rather than downgrading the toolchain to make a
   gate go green.
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

7. **Prove reuse before you build.** The default answer is that the platform
   already has it and your change fills a gap or adds a helper on it, so the PR
   body names what you reused and what you deliberately did not. A second
   mechanism for a job one seam already does is refused however clean it is.
   Two tells you are reinventing: what you are writing re-answers a question an
   existing seam answered, and your reason for its placement is that a
   convenient loop was already there rather than that the responsibility
   belongs to it.

8. **Build the simplest thing that works; size is the signal.** A coding task's
   budget is about **500 hand-written lines** — generated output does not
   count, tests count apart — and past **2,000** the change has become a second
   system, so **stop and ask the user** instead of finishing it. Two tells,
   checkable before the code exists: you cannot name the one responsibility the
   change adds, and it is growing to make the end goal pass rather than to put
   a behaviour where it belongs. Wanting it to work licenses no means to get
   there.

## Where the depth is

- `README.md` — API, byte path, Watch delivery, readiness semantics (including
  why `stat` is the weaker probe), authentication, local vs deployed profiles,
  supply-chain evidence.
- `docs/local-minio-recovery.md` — local MinIO custody, retained volumes and
  recovery.
- `.claude/skills/local-test-suites/` — running the integration and e2e suites
  against real containers, and proving they actually ran.
