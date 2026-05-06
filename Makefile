SHELL := /bin/bash

GO_IMAGE ?= golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0
GO_RUN := docker run --rm -v "$(CURDIR)":/app -w /app $(GO_IMAGE)

.PHONY: up down logs ps test build verify verify-full compose-config bootstrap-verify e2e

up:
	cp -n .env.example .env || true
	docker compose up --build -d

down:
	docker compose down --remove-orphans --volumes

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

test:
	$(GO_RUN) go test ./...

build:
	$(GO_RUN) sh -c 'CGO_ENABLED=0 go build ./...'

compose-config:
	docker compose config >/dev/null

bootstrap-verify:
	docker compose run --rm bootstrap verify

e2e:
	$(GO_RUN) go test ./test/e2e/... -tags=e2e -v -timeout=10m

verify:
	./scripts/verify.sh

verify-full:
	FULL=1 ./scripts/verify.sh
