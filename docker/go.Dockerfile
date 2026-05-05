FROM golang:1.23

WORKDIR /app

COPY . .

ARG BIN

RUN CGO_ENABLED=0 go build -o /usr/local/bin/app ./cmd/${BIN}

ENTRYPOINT ["/usr/local/bin/app"]
