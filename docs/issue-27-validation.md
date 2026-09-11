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

## Linux ownership follow-up

The earlier macOS passes did not establish Linux success. Full logs from
[run 34635423818](https://github.com/codefly-dev/service-object-storage/actions/runs/34635423818)
show all three failing E2E tests reached a Linux `TempDir RemoveAll` permission
error: `TestMinIOPersistentCustody` and `TestRuntimeEndToEndPutGet` could not unlink
object `xl.meta`; `TestMinIOMissingBucketFailsClosed` could not unlink
`.minio.sys/config/config.json/xl.meta`. MinIO ran as image-default root and wrote
root-owned files inside the agent user's private bind mount. Docker Desktop's
ownership translation masked this defect locally.

The Runtime now reuses Core's `WithUser` to run MinIO with the agent's effective
UID:GID. It does not modify ownership or permissions of retained data. The three
regressions inspect the actual container user (including replacement) and, after
container teardown, walk the complete retained data tree as the host user. They
verify read/write access to real MinIO metadata and directory management without
privileged cleanup. Existing byte, metadata, key, token-revocation and fail-closed
assertions remain in force. Applying these tests to the previous Runtime via a
Go source overlay fails on the missing container user even on macOS.

Follow-up local build, vet (including e2e), tidy, race tests and gateway image
build passed. Broader e2e-tagged lint still reports the same 13 existing findings.
Initial local test attempts encountered transient Docker port-binding failures;
another attempt started before the new image tag was available. Those attempts
failed and were not counted as validation. The full suite was rerun after the
image build completed and passed all E2E tests with zero skips (36.285s). An
intermediate GCS projection attempt failed on an empty projection path; the
positive Runtime tests now assert Init READY with its error message, so future
initialization failures cannot be hidden behind that secondary assertion.
The final full run includes a passing GCS projection test. Final local and hosted
Linux results are recorded on
[PR #28](https://github.com/codefly-dev/service-object-storage/pull/28) and its
commit-specific Checks tab; the failed run above is not treated as green.

## Review fixes: structured custody, generation fencing and bootstrap state

Four additional review failures were implemented after the earlier green run:

- Custody v2 hashes a JSON tuple of workspace/module/service/scope, and records
  that tuple. The colliding `documents-archive/object-storage` and
  `documents/archive-object-storage` identities cannot accept each other's data.
  Incumbent conflicts and ambiguous v1 directories fail without replacement.
- Core's name-based `Shutdown` could delete a successor. Dependency
  [Core PR462](https://github.com/codefly-dev/core/pull/462), pinned at
  `69492a82459027ec36d64b0f291a9043b4c99dfa`, removes only its acquired container
  ID while retaining log cleanup. The agent holds custody through gateway Init.
- Pending provisioning resumes after interruption before container creation;
  atomic, synced record commitment follows successful bucket provisioning.
  Missing committed buckets and missing custody evidence still fail closed.
- Docker rootless/userns mapping is rejected before custody mutation. Mock
  daemon tests verify even the lock directory is absent after rejection.
  No live rootless daemon was available; this is an explicit unsupported-mode
  rejection, not a claim of rootless filesystem compatibility.

Local build, vet (including e2e), tidy, all race tests and full E2E passed.
Full E2E took 38.013s, with no skipped tests. It reads authenticated objects after
stale Runtime teardown, checks exact bytes/metadata and credential rotation,
resumes interrupted bootstrap and rejects identity collisions/legacy custody.
The separate Docker runner race suite passed in 89.629s. Its new regression
fails against unmodified Core: recorded stop/remove requests target the
successor instead of the acquired ID.

Full golangci-lint remains failing on 13 existing agent findings (with e2e),
and 28 existing findings in Core's owning Docker runner package. No lint
suppressions or unrelated cleanup were added. These are not green lint claims.
No shared Wiki containers, retained volumes, keys or local document data were
modified. Recovery/adoption remains separate under the Wiki handoff.
