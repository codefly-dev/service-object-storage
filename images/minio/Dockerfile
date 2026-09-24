# syntax=docker/dockerfile:1.7
#
# MinIO server, built from MinIO's public AGPL-3.0 source at one release commit.
#
# Why this exists: the agent runs MinIO for local and test stores, and every
# registry MinIO publishes to stopped serving anonymous pulls (Docker Hub
# `minio/minio` first, then quay.io/minio/minio and quay.io/minio/mc, which now
# answer 401 even to a manifest read). publish-minio-image.yml builds this file
# and pushes the result to ghcr.io/codefly-dev/minio; minio-image.json records
# the digest the agent, CI and the docs pin.
#
# The source is unmodified. The binary is built the way upstream's Makefile
# builds it (`make build`: CGO off, `-tags kqueue`, `-trimpath`, ldflags from
# upstream's own buildscripts/gen-ldflags.go), so `minio --version` reports the
# upstream release tag and commit. The runtime stage follows upstream's
# Dockerfile.release — same base image family, environment, entrypoint script
# and /licenses — minus the `mc` and static `curl` binaries it downloads from
# dl.min.io: nothing in this repository runs either inside the container.
#
# Reproducible: every input is pinned (source by commit, Go toolchain and base
# image by digest, modules by upstream's go.sum), the Go build is cross-compiled
# on the build platform so both architectures come from the same toolchain, and
# the publishing workflow sets SOURCE_DATE_EPOCH to the upstream commit time
# with rewrite-timestamp, so a rebuild of the same pins yields the same digest.

# Go toolchain: go.mod at the pinned commit names `toolchain go1.24.2`.
ARG GOLANG_IMAGE=golang:1.24.2-alpine@sha256:7772cb5322baa875edd74705556d08f0eeca7b9c4b5367754ce3f2f00041ccee
# Upstream's Dockerfile.release runtime base (ubi9/ubi-micro), pinned by digest.
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi9/ubi-micro:latest@sha256:7a0454cbd9bd847e8f6a63b6f0254a6efbeb6e0ed71a5d824a4f6cccbe626650

FROM --platform=$BUILDPLATFORM ${GOLANG_IMAGE} AS build

# The upstream release tag and the commit it points at. The build refuses to
# continue unless the fetched tree is exactly that commit.
ARG MINIO_RELEASE_TAG=RELEASE.2025-04-22T22-12-26Z
ARG MINIO_COMMIT=0d7408fc9969caf07de6a8c3a84f9fbb10a6739e
ARG MINIO_SOURCE=https://github.com/minio/minio.git
ARG TARGETOS
ARG TARGETARCH

ENV CGO_ENABLED=0 \
    GOTOOLCHAIN=local \
    GOFLAGS=-mod=readonly

# git is a build-stage tool only: gen-ldflags.go reads the commit id from it.
RUN apk add --no-cache git ca-certificates

WORKDIR /src
RUN git init -q . \
    && git remote add origin "${MINIO_SOURCE}" \
    && git fetch -q --depth 1 origin "${MINIO_COMMIT}" \
    && git checkout -q --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "${MINIO_COMMIT}"

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && go mod verify

# The version argument is the release tag's own timestamp in RFC 3339, which is
# what upstream's release build passes; MINIO_RELEASE=RELEASE makes
# gen-ldflags.go derive ReleaseTag=RELEASE.<timestamp> from it.
RUN --mount=type=cache,target=/go/pkg/mod \
    set -eu; \
    stamp="${MINIO_RELEASE_TAG#RELEASE.}"; \
    version="$(echo "${stamp}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z$/T\1:\2:\3Z/')"; \
    ldflags="$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${version}")"; \
    case "${ldflags}" in *"ReleaseTag=${MINIO_RELEASE_TAG} "*) ;; *) echo "ldflags do not carry ${MINIO_RELEASE_TAG}: ${ldflags}" >&2; exit 1;; esac; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
      go build -tags kqueue -trimpath -ldflags "${ldflags}" -o /out/minio . ; \
    mkdir -p /out/licenses; \
    cp LICENSE CREDITS /out/licenses/; \
    cp dockerscripts/docker-entrypoint.sh /out/docker-entrypoint.sh

FROM ${RUNTIME_IMAGE}

ARG MINIO_RELEASE_TAG=RELEASE.2025-04-22T22-12-26Z

# Upstream's Dockerfile.release environment, less MINIO_UPDATE_MINISIGN_PUBKEY:
# this binary is not upstream's signed release, so `mc admin update` must not
# be pointed at replacing it.
ENV MINIO_ACCESS_KEY_FILE=access_key \
    MINIO_SECRET_KEY_FILE=secret_key \
    MINIO_ROOT_USER_FILE=access_key \
    MINIO_ROOT_PASSWORD_FILE=secret_key \
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \
    MINIO_CONFIG_ENV_FILE=config.env

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build --chmod=0755 /out/minio /usr/bin/minio
COPY --from=build --chmod=0755 /out/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
COPY --from=build /out/licenses/ /licenses/

EXPOSE 9000
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]
