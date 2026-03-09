FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -o /out/control-plane ./cmd/control-plane

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/control-plane /control-plane
ENTRYPOINT ["/control-plane"]
