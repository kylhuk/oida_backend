SHELL := /bin/bash

GO_IMAGE ?= golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0
GO_RUN := docker run --rm -v "$(CURDIR)":/app -w /app $(GO_IMAGE)
COMPOSE := COMPOSE_PROFILES=live-crawl docker compose

.PHONY: up down logs ps test build verify verify-full compose-config bootstrap-verify e2e

up:
	cp -n .env.example .env || true
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down --remove-orphans --volumes

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

test:
	$(GO_RUN) go test ./...

build:
	$(GO_RUN) sh -c 'CGO_ENABLED=0 go build ./...'

compose-config:
	$(COMPOSE) config >/dev/null

bootstrap-verify:
	$(COMPOSE) run --rm bootstrap verify

e2e:
	$(GO_RUN) go test ./test/e2e/... -tags=e2e -v -timeout=10m

verify:
	./scripts/verify.sh

verify-full:
	FULL=1 ./scripts/verify.sh
