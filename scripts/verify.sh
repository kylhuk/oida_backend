#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_IMAGE="${GO_IMAGE:-golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0}"

cd "${ROOT}"

run_go() {
  docker run --rm -v "${ROOT}:/app" -w /app "${GO_IMAGE}" "$@"
}

run_go_host() {
  docker run --rm --network host -v "${ROOT}:/app" -w /app "${GO_IMAGE}" "$@"
}

echo "== compose config =="
docker compose config >/dev/null

echo "== go test ./... =="
run_go go test ./...

echo "== static build =="
run_go sh -c 'CGO_ENABLED=0 go build -buildvcs=false ./...'

echo "== generated API docs are current =="
run_go go test ./cmd/api -run 'TestGeneratedDocsAreCurrent|TestRouteContract'

if [[ "${FULL:-0}" == "1" ]]; then
  echo "== reset compose stack =="
  docker compose down --remove-orphans --volumes

  echo "== full compose stack =="
  docker compose up -d --build

  echo "== wait for API readiness =="
  timeout=180
  until curl -fsS http://localhost:8080/v1/ready >/tmp/oida-ready.json; do
    timeout=$((timeout - 5))
    if [[ "${timeout}" -le 0 ]]; then
      docker compose logs --tail=200
      exit 1
    fi
    sleep 5
  done

  echo "== bootstrap verify =="
  docker compose run --rm bootstrap verify

  echo "== e2e =="
  run_go_host go test ./test/e2e/... -tags=e2e -v -timeout=10m
fi

echo "verification complete"
