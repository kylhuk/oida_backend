FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -o /out/bootstrap ./cmd/bootstrap

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/bootstrap /bootstrap
COPY migrations /app/migrations
ENTRYPOINT ["/bootstrap"]
