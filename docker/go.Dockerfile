FROM golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BIN
RUN test -n "${BIN}" && \
    mkdir -p /out /runtime/ready /runtime/tmp && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${BIN}

FROM gcr.io/distroless/static-debian12:nonroot@sha256:a9329520abc449e3b14d5bc3a6ffae065bdde0f02667fa10880c49b35c109fd1

WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /runtime/ready /ready
COPY --from=build --chown=nonroot:nonroot /runtime/tmp /tmp
COPY --from=build --chown=nonroot:nonroot /src/migrations /app/migrations
COPY --from=build --chown=nonroot:nonroot /src/seed /app/seed
COPY --from=build --chown=nonroot:nonroot /src/infra/backup /app/infra/backup
COPY --from=build --chown=nonroot:nonroot /src/sources.md /app/sources.md
COPY --from=build --chown=nonroot:nonroot /src/sources2.md /app/sources2.md
COPY --from=build /out/app /usr/local/bin/app

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/app"]
