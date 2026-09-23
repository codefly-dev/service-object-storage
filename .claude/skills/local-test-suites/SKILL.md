---
name: local-test-suites
description: Run this repo's integration and end-to-end suites against real MinIO and a real gateway image, and prove they actually executed. Use when a change touches internal/backend, internal/cache, the readiness probe, the agent Runtime or Builder, or the image-SBOM path — anywhere `go test ./...` alone does not reach.
---

# Running the real-container suites

`go test ./...` covers the `mem` backend and a miniredis cache. It never opens a
socket to MinIO, never starts a container, and never inventories an image. Three
tiers exist, and only the tier you actually ran is evidence:

| Tier | Command shape | What it proves |
|------|---------------|----------------|
| unit | `go test ./...` | logic, `mem` backend, cache behaviour |
| integration | `-tags integration` + live MinIO | the gRPC server against a real S3-compatible store |
| e2e | `-tags e2e` + a built gateway image | the agent Runtime's container lifecycle, GCS key projection, MinIO custody, image SBOM |

## Integration

```bash
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e server /data
docker run -d --name fake-gcs -p 4443:4443 \
  fsouza/fake-gcs-server@sha256:d47b4cf8b87006cab8fbbecfa5f06a2a3c5722e464abddc0d107729663d40ec4 \
  -scheme http -public-host 127.0.0.1:4443
for i in $(seq 1 30); do curl -sf http://127.0.0.1:9000/minio/health/live && break; sleep 1; done

MINIO_ENDPOINT=127.0.0.1:9000 MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin MINIO_BUCKET=sos-test \
STORAGE_EMULATOR_HOST=127.0.0.1:4443 GCS_BUCKET=sos-test \
  go test -tags integration -count=1 -v ./internal/integration/...

docker rm -f minio fake-gcs
```

Pull from quay.io by digest. Docker Hub's `minio/minio` repository no longer
serves anonymous pulls, and this digest is the image the agent's Runtime pins,
so the suite and the Runtime exercise one MinIO version.

Six tests run: `TestMinIOFullStack`, `TestMinIOConditionalDelete`,
`TestProbeAgainstLiveMinIO` (whose subtests cover granted access, a missing
bucket and refused credentials for both probe strategies), and against
fake-gcs-server `TestGCSFullStack`, `TestGCSPrefixConfinesKeys` and
`TestGCSProbe`. The emulator accepts any caller, so the GCS tests prove the
keyless client path and the prefix, never Workload Identity or a bucket grant.

## Proving the run happened

The integration suite **skips silently**. With `MINIO_ENDPOINT` unset every test
calls `t.Skip`, the package prints `ok`, and `go test` exits `0`:

```
$ go test -tags integration ./internal/integration/...
ok  	github.com/codefly-dev/service-object-storage/internal/integration	0.391s   # nothing ran
```

So `ok` is not a result. Count what passed:

```bash
go test -tags integration -count=1 -v ./internal/integration/... 2>&1 | grep -c '^--- PASS'   # expect 6
go test -tags integration -count=1 -v ./internal/integration/... 2>&1 | grep -c '^--- SKIP'   # expect 0
```

`-count=1` matters: a cached pass replays as `ok` too.

Dropping the build tag fails honestly instead — `matched no packages`, exit 1 —
so a missing tag is the error you will notice and a missing endpoint is the one
you will not.

To confirm the suite is really reaching the store rather than passing on
something local, point it at a dead port and watch it fail:

```bash
MINIO_ENDPOINT=127.0.0.1:9999 MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin MINIO_BUCKET=sos-test \
  go test -tags integration -count=1 ./internal/integration/...
# dial tcp 127.0.0.1:9999: connect: connection refused  → FAIL
```

## End-to-end

Needs a gateway image built from this tree, and `syft` on PATH for the SBOM
test. The MinIO tag is aliased because the Runtime resolves it by tag while
Docker Hub will not serve it:

```bash
docker build -t service-object-storage:e2e .
docker pull quay.io/minio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e
docker tag quay.io/minio/minio@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e \
  quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z

SOS_GATEWAY_IMAGE=service-object-storage:e2e \
  go test -tags e2e -count=1 -timeout 300s .
```

`-timeout 300s` is what CI uses on a dedicated runner; the whole suite completes
in roughly 80s on an idle local daemon. It starts and stops real containers, so
it is not a fast loop — narrow it while iterating: `-run TestImageSBOM` for the
evidence path, `-run TestRuntimeEndToEnd` for the put/get lifecycle,
`-run TestMinIO` for the custody proofs.

**An overloaded Docker daemon fails this suite in a way that reads like a
product bug.** The daemon is shared with every other agent session on the
machine. Under load, the gateway starts and logs `backend access: SERVING`, and
the test still fails with `rpc error: code = DeadlineExceeded`, while teardown
logs `Docker.Stop: cannot stop container … context deadline exceeded` and the
next custody test hangs past the timeout. Nothing in that output points at
Docker. Before believing an e2e failure, count what else is running —
`docker ps -a | wc -l` — and re-run the failing test alone on a quiet daemon.

This tag is **not** uniformly skip-safe. Without `SOS_GATEWAY_IMAGE` the custody
tests fail loudly (`Should NOT be empty`), while `TestRuntimeEndToEndPutGet`,
`TestGCSCredentialProjection`, `TestGatewayStartFailureIsOwnedByRuntime` and
`TestImageSBOMInventoriesTheGatewayImage` skip. A partially-green `e2e` run can
therefore mean the image was never set — check for `SKIP` before reading a pass
as coverage. `TestImageSBOMInventoriesTheGatewayImage` also skips when `syft` is
absent, which is the quiet one: nothing about the failure mentions the image.

## Container hygiene

The Docker daemon is shared with other repos' agent sessions. Interrupted or
failed runs leave containers named `codefly-test-mod-svc-*` behind — a failed
`-tags e2e` run leaves one within seconds. List before you remove, and never
blanket-prune:

```bash
docker ps -a --filter 'name=codefly-test-mod-svc' --format '{{.Names}}\t{{.Status}}'
```

Containers older than your session belong to someone else's run. The custody
tests point `CODEFLY_HOME` at a temp dir, so they select no shared stack and
retain no volume of yours; the containers are the part that outlives them.
