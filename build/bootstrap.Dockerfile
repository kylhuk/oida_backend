FROM golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/bootstrap ./cmd/bootstrap

FROM gcr.io/distroless/static-debian12@sha256:20bc6c0bc4d625a22a8fde3e55f6515709b32055ef8fb9cfbddaa06d1760f838
COPY --from=build /out/bootstrap /bootstrap
COPY infra /app/infra
COPY migrations /app/migrations
COPY seed /app/seed
COPY sources.md /app/sources.md
ENTRYPOINT ["/bootstrap"]
