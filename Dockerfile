# The object-storage gateway image. A client speaks only the codefly/storage/v0
# gRPC contract; every cloud backend (S3/GCS/Azure/MinIO) is compiled in and
# selected at runtime by SOS_BACKEND. The image ships no cloud credentials —
# those arrive as environment at deploy time.
# The toolchain is the gateway's standard library. go.mod's `go 1.27.3` already
# refuses an older one (the image sets GOTOOLCHAIN=local); the build stage is
# pinned to that patch release by digest so the gateway build is reproducible
# rather than resolved from a floating `golang:1.27` tag.
FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
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
