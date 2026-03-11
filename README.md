# Global OSINT Backend

A **production-ready**, **Go-first**, **ClickHouse-centric** OSINT platform for acquiring, structuring, and serving public intelligence data at scale.

[![CI](https://github.com/yourorg/global-osint-backend/workflows/Docker%20Compose%20Integration%20Tests/badge.svg)](https://github.com/yourorg/global-osint-backend/actions)
[![Go Version](https://img.shields.io/badge/go-1.23-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Overview

This platform ingests public OSINT from structured APIs, web crawling, and broad-web corpora, then structures it into canonical facts, entities, places, and events. Every record resolves to a **location anchor** with continent → admin4 hierarchy.

### Key Capabilities

- **Multi-Domain Intelligence**: Maritime (AIS/vessels), Aviation (ADS-B/aircraft), Space (satellites/orbits), Geopolitical (conflicts/events), Safety/Security (sanctions/hazards)
- **Global Place Graph**: Reverse geocoding, admin boundaries, H3 coverage
- **Metrics & Analytics**: 50+ metrics with place-based rollups from local to world level
- **Production Scale**: Single-node baseline with optional ClickHouse Keeper cluster
- **Data Governance**: Source kill switches, license tracking, retention policies

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        REST API (Go)                         │
│  /v1/sources  /v1/places  /v1/events  /v1/metrics            │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Control Plane (Go)                        │
│  Run-once jobs: place-build, promote, ingest-* packs         │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
│ worker-fetch   │  │ worker-parse    │  │ renderer        │
│ HTTP crawling  │  │ JSON/CSV/XML    │  │ Browser (Chromium)
└────────────────┘  └─────────────────┘  └─────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   ClickHouse (Analytics DB)                  │
│  meta: registries        │  ops: frontier/jobs/logs          │
│  bronze: raw docs        │  silver: canonical facts          │
│  gold: metrics/snapshots │                                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      MinIO (Object Store)                    │
│  Raw payloads, WARC files, backups, staging                  │
└─────────────────────────────────────────────────────────────┘
```

> The renderer serves the internal stats dashboard on `http://localhost:8090/` and keeps `/health` for service checks.

## Quick Start

### Prerequisites

- Docker Compose 2.20+
- 8GB RAM minimum (16GB recommended)
- 50GB disk space

### One-Command Bootstrap

```bash
# Start all services
docker compose up -d --build

# Verify local HTTP fixture service used by E2E
curl http://localhost:8079/health.json

# List supported orchestration jobs
go run ./cmd/control-plane run-once --help

# Verify installation
docker compose run --rm bootstrap verify

# Check health
curl http://localhost:8080/v1/health
curl http://localhost:8080/v1/ready
```

### API Endpoints

**Health & Control:**
- `GET /v1/health` - Service health
- `GET /v1/ready` - Bootstrap completion status
- `GET /v1/version` - API version
- `GET /v1/schema` - Available endpoints

**Sources & Places:**
- `GET /v1/sources` - List configured sources
- `GET /v1/sources/{id}` - Source details
- `GET /v1/places` - Geographic places
- `GET /v1/places/{id}/children` - Place hierarchy
- `GET /v1/places/{id}/metrics` - Place analytics

**Events & Entities:**
- `GET /v1/events` - OSINT events
- `GET /v1/entities` - Canonical entities (vessels, aircraft, etc.)
- `GET /v1/entities/{id}/tracks` - Movement tracks
- `GET /v1/observations` - Raw observations

**Analytics:**
- `GET /v1/metrics` - Available metrics
- `GET /v1/analytics/rollups` - Aggregated data
- `GET /v1/analytics/time-series` - Temporal analysis
- `GET /v1/analytics/hotspots` - Risk hotspots
- `GET /v1/search` - Cross-domain search

See [API Documentation](docs/api-reference.md) for complete specification.

### Control-Plane Jobs

- `place-build`
- `promote`
- `ingest-geopolitical`
- `ingest-maritime`
- `ingest-aviation`
- `ingest-space`
- `ingest-safety-security`

## Domain Packs

### Geopolitical Pack (Task 20)
Sources: GDELT, ReliefWeb, ACLED (credential-gated)
Metrics: conflict_intensity_score, cross_border_spillover_score, humanitarian_pressure_score, infrastructure_disruption_score, media_attention_acceleration, media_attention_score, protest_activity_score, sanction_activity_score

### Maritime Pack (Task 22)
Sources: Public AIS, vessel registries, port databases
Current metrics: ais_dark_hours_sum, anchorage_dwell_hours, flag_registry_mismatch_score, port_gap_hours, route_deviation_score, shadow_fleet_score

### Aviation Pack (Task 23)
Sources: OpenSky ADS-B, aircraft registries, NOTAMs
Metrics: altitude_variance_score, diversion_rate, hold_pattern_frequency, military_aircraft_proximity_score, military_likelihood_score, route_irregularity_score, squawk_change_rate, transponder_gap_hours

### Space Pack (Task 24)
Sources: TLE/OMM feeds, satellite catalogs
Current metrics: overpass_density, conjunction_risk

### Safety/Security Pack (Task 25)
Sources: OpenSanctions, NASA FIRMS, KEV catalog
Current metrics: sanctions_exposure, fire_hotspot

## Development

### Project Structure

```
.
├── cmd/
│   ├── api/              # REST API server
│   ├── bootstrap/        # One-shot initialization
│   ├── control-plane/    # Job orchestration
│   ├── worker-fetch/     # HTTP crawler
│   ├── worker-parse/     # Content parser
│   └── renderer/         # Internal stats dashboard + health endpoint
├── internal/
│   ├── canonical/        # Data envelopes & IDs
│   ├── discovery/        # URL discovery (robots, sitemaps)
│   ├── fetch/            # HTTP client with retention
│   ├── location/         # Geocoding & attribution
│   ├── metrics/          # Analytics framework
│   ├── parser/           # Content parsers
│   ├── place/            # Place graph materialization
│   ├── packs/*/          # Domain packs (5 domains)
│   ├── promote/          # Data pipeline
│   └── resolution/       # Entity deduplication
├── migrations/clickhouse/    # SQL migrations
├── seed/                 # Initial data
├── testdata/fixtures/    # Test fixtures
├── docs/
│   ├── runbooks/         # Operational guides
│   ├── dashboards/       # Quality monitoring
│   └── schema-standards.md
└── infra/
    └── clickhouse/cluster/  # Scale-out config
```

### Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package tests
go test ./internal/location/...
go test ./internal/packs/geopolitical/...

# Run contract tests
go test ./cmd/api -run 'Test.*Contract'

# Run E2E tests
go test ./test/e2e/... -tags=e2e
```

### Adding a Domain Pack

1. Create `internal/packs/<domain>/` directory
2. Implement adapter interfaces:
   - `SourceAdapter` - Fetch from external API
   - `Normalizer` - Transform to canonical schema
   - `MetricCalculator` - Compute domain metrics
3. Add tests with fixtures
4. Register in `cmd/control-plane/jobs_<domain>.go`
5. Update `seed/source_registry.json`

## Operations

### Runbooks

- [Fresh Bootstrap](docs/runbooks/fresh-bootstrap.md) - First-time setup
- [Upgrade Migration](docs/runbooks/upgrade-migration.md) - Schema updates
- [Backup/Restore](docs/runbooks/backup-restore.md) - Disaster recovery
- [Kill Switch](docs/runbooks/kill-switch.md) - Emergency source disable
- [Unresolved Triage](docs/runbooks/unresolved-triage.md) - Failed geolocation queue
- [Cluster Scale-Out](docs/runbooks/cluster-scale-out.md) - Production clustering

### Quality Dashboards

Monitor system health via:
- Source freshness lag
- Parser success rates
- Source-catalog rollout counts (concrete, fingerprint, family)
- Runnable vs deferred vs credential-gated source visibility

Internal dashboard endpoints:
- UI: `http://localhost:8090/`
- Stats JSON: `http://localhost:8080/v1/internal/stats`

Notes:
- Curated large-table row counts are labeled `approximate`.
- MVP excludes geolocation map and schema-drift panels.
- Catalog rollout visibility lives in the `summary` payload under `catalog_total`, `catalog_concrete`, `catalog_fingerprint`, `catalog_family`, `catalog_runnable`, `catalog_deferred`, and `catalog_credential_gated`.
- Concrete sources are either runtime-linked runnable rows or explicitly deferred. Fingerprints and families are review-gated generators, not runnable sources.
- Deferred websocket, login-required, browser-driven, and other interactive transports remain visible in catalog counts as deferred sources; they are not automated by the current sync loop.

See [Quality Dashboards](docs/dashboards/quality-dashboards.md)

## Scale-Out (Optional)

For production workloads beyond single-node capacity:

```bash
# Start cluster topology
docker compose -f docker-compose.cluster.yml up -d

# Apply cluster schema
./infra/clickhouse/cluster/scripts/apply-cluster-schema.sh

# Run cluster tests
./infra/clickhouse/cluster/scripts/cluster-happy-path.sh
```

Topology:
- 3× ClickHouse Keeper (quorum)
- 4× ClickHouse servers (2 shards × 2 replicas)
- ReplicatedMergeTree + Distributed tables

## Production Readiness

See [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) for deployment checklist.

## Contributing

1. Follow [Schema Standards](docs/schema-standards.md)
2. Add tests for new functionality
3. Update evidence files for task completion
4. Run full CI suite before PR

## License

MIT License - See [LICENSE](LICENSE)

## Acknowledgments

- geoBoundaries for global admin boundaries
- GeoNames for place gazetteer
- GDELT Project for event data
- OpenSanctions for entity graph
