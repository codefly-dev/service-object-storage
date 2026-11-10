# The object-storage gateway image. A client speaks only the codefly/storage/v0
# gRPC contract; every cloud backend (S3/GCS/Azure/MinIO) is compiled in and
# selected at runtime by SOS_BACKEND. The image ships no cloud credentials —
# those arrive as environment at deploy time.
# The toolchain is the gateway's standard library, so it is pinned to the
# patch release go.mod names, by digest: a floating `golang:1.27` built the
# af340eaf gateway on 1.27.1 and shipped its HIGH stdlib advisories
# (CVE-2026-78667, CVE-2026-97031, CVE-2026-111943; fixed in 1.27.2/1.27.3).
FROM golang:1.27.3@sha256:e519d3b0a5b285e5aa53ab9eef02bc6bbb9cb04608389b743a136a9ce75f0d1e AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/service-object-storage ./cmd/service-object-storage

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/service-object-storage /service-object-storage
# The gRPC listen port; SOS_LISTEN can override it.
EXPOSE 9464
ENTRYPOINT ["/service-object-storage"]
