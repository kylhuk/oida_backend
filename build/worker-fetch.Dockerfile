FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -o /out/worker-fetch ./cmd/worker-fetch

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/worker-fetch /worker-fetch
ENTRYPOINT ["/worker-fetch"]
