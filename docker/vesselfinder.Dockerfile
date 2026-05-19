FROM golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker-vesselfinder ./cmd/worker-vesselfinder

FROM debian:bookworm-slim@sha256:df52e55e3361a81ac1bead266f3373ee55d29aa50cf0975d440c2be3483d8ed3

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates chromium fonts-liberation && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 65532 --gid nogroup --home-dir /nonexistent --shell /usr/sbin/nologin nonroot && \
    mkdir -p /chrome-profile && chown 65532:65534 /chrome-profile
COPY --from=build /out/worker-vesselfinder /usr/local/bin/worker-vesselfinder

USER nonroot:nogroup
ENV HOME=/tmp
ENTRYPOINT ["/usr/local/bin/worker-vesselfinder"]
