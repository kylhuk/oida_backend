# Incoming Task

## Request

Continue the Go OSINT backend and make it production ready.

## Locked Decisions

- The Go/ClickHouse OSINT backend is the only active product.
- The legacy Python/FastAPI medallion stack is removed, not maintained.
- The first production target is the existing single-node Docker topology.
- Hashed API keys are the first production authentication model.

## Non-Goals

- Kubernetes manifests.
- OIDC/JWT identity-provider integration.
- A custom frontend beyond Grafana operational dashboards.
