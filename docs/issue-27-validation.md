# Issue 27 validation

Local validation on 2026-09-11, macOS arm64, Go 1.27.1, Docker 29.4.0.
Baseline: `202dd353816b583fc3ed4a81aa1aef98b161da38`.

| Check | Result |
| --- | --- |
| `go build ./...` | Passed |
| `go vet ./...` and `go vet -tags e2e ./...` | Passed |
| `go mod tidy -diff` | Passed, no diff |
| `go test ./...` and `go test -race ./...` | Passed |
| `docker build -t service-object-storage:issue27-proof .` | Passed |
| `SOS_GATEWAY_IMAGE=service-object-storage:issue27-proof go test -v -tags e2e -count=1 -timeout 300s .` | Passed, full suite, no skipped tests |
| `go test -v -tags integration -count=1 ./internal/integration/...` | Passed against a disposable MinIO, no skipped tests |
| `golangci-lint run ./...` (v2.13.1) | Failed: 12 existing findings, reproduced on an unchanged baseline archive |
| `golangci-lint run --build-tags e2e ./...` | Failed: the same 12 plus the existing unchecked connection Close in `runtime_e2e_test.go:124` |

The linter findings are five unchecked Close results (cache, events, server and
existing runtime port allocation), three redundant embedded-field selectors
(builder/main), and four deprecated SDK usages (GCS and S3). Path/line-normalized
findings match the baseline exactly. No lint suppression or authorization change
was added. The owning reusable source workflow gates build, vet, tidy and tests;
the additional broader linter is **not green**.

`TestMinIOPersistentCustody` drives real Builder/Runtime Load, Init and Start,
with a temporary `CODEFLY_HOME` and unique service name. It writes two keys,
including binary data, through the authenticated gateway and compares exact
bytes and the complete Stat metadata. It repeats after agent Stop/Start, actual
Docker Stop/Start, and a fresh Runtime's configuration-triggered recreation.
The new MinIO container has a different ID; the old container is gone; both
credentials rotate and the old gateway token is rejected. The same store remains
readable. Missing and mismatched markers and a missing record return Init ERROR
without removing the running containers or changing their objects.

`TestMinIOLegacyVolumeRefusesReplacement` creates a disposable anonymous volume,
writes a sentinel, then verifies the agent refuses replacement even with
bootstrap enabled. The container and bytes remain intact and no replacement
directory is created. `TestMinIOMissingBucketFailsClosed` removes only an empty
disposable bucket and verifies recreation reports loss without re-creating it.
Unit coverage includes missing data, owner/bucket mismatch, malformed records,
symlinks, partial initialization and incorrect mounts.

The separate integration run used a random `sos27-integration-<uuid>` container
and named volume, a random disposable root password, and a dynamically allocated
loopback port. Cleanup removed only those exact test resources. Runtime tests
also remove only their own containers and anonymous volume; temporary data
locations belong to the test framework. No Wiki container, retained incident
volume, key or local document data was modified. No shared-stack restart, prune,
merge, deployment or Wiki restoration was performed.

Recovery and adoption remain separate work under
[handoff #133](https://github.com/obin-ai/core-solutions/issues/133).
See [recovery instructions](local-minio-recovery.md): retain and copy the source
read-only into a separately owned candidate, preserve filesystem metadata and
versions, and verify authenticated reads before scheduling any shared rollout.
