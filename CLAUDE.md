# AI LongList Builder — Competitive Intelligence Platform

## What We're Building

An AI-powered competitive intelligence and peer benchmarking platform (similar to StrategyBridge.ai).
Users can discover companies, find their market peers, and benchmark them on key metrics.
Target scale: 50M+ company records.

## Learning Goals

This project is intentionally designed to teach:
- **Microservices** — 4 independent services, each with a single responsibility
- **Kafka** — async event-driven pipeline between services
- **Elasticsearch** — full-text search + KNN vector similarity for peer discovery
- **OpenTelemetry + Jaeger + Grafana** — distributed tracing and observability
- **Terraform** — infrastructure as code (AWS deployment, tackled last)

## Architecture (4 Microservices)

```
External APIs / CSVs
       │
  [collector]     → Kafka: raw-companies
       │
  [enrichment]    → Claude AI (tags, summary, embedding) → Kafka: enriched-companies
       │
  [indexer]       → Elasticsearch (full-text + KNN vector fields)
       │
  [search-api]    → FastAPI: /search, /peers/{id}, /benchmark/{id}
```

All services emit OpenTelemetry traces → Jaeger.
Prometheus scrapes metrics → Grafana dashboards.

## Project Structure

```
ai-longlist-builder/
├── services/
│   ├── collector/          # ingests from external APIs, publishes to Kafka
│   ├── enrichment/         # consumes raw-companies, calls Claude, publishes enriched-companies
│   ├── indexer/            # consumes enriched-companies, writes to Elasticsearch
│   └── search-api/         # FastAPI: search, peer discovery, benchmarking endpoints
├── infrastructure/
│   └── terraform/          # AWS: ECS Fargate, MSK, OpenSearch, ElastiCache
├── docker-compose.yml      # local dev: ES, Kafka, Jaeger, Prometheus, Grafana
└── CLAUDE.md
```

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Services | Python 3.12 + FastAPI | Fast, async, great ES/Kafka clients |
| Message bus | Apache Kafka | Decouples services, replay-able, scales horizontally |
| Search & vector | Elasticsearch 8.x | Full-text + KNN in one engine, handles 50M docs |
| AI enrichment | Claude API (claude-sonnet-4-6) | Tags, summaries, embeddings per company |
| Cache | Redis | Benchmark aggregations cached 1h, peer lists 6h |
| Tracing | OpenTelemetry → Jaeger | Distributed trace across all 4 services |
| Metrics | Prometheus + Grafana | Ingestion rate, consumer lag, search latency |
| Infrastructure | Terraform + AWS | ECS Fargate, MSK, OpenSearch, ElastiCache |
| Containers | Docker + docker-compose | Local dev environment |

## Key Data Model (Elasticsearch)

```json
{
  "id": "string",
  "name": "string",
  "domain": "string",
  "description": "text",
  "ai_summary": "text (Claude-generated)",
  "tags": ["keyword"],
  "industry": "keyword",
  "business_model": "keyword (B2B/B2C/B2B2C)",
  "country": "keyword",
  "city": "keyword",
  "founded_year": "integer",
  "employee_count": "integer",
  "funding_stage": "keyword",
  "total_funding_usd": "long",
  "embedding": "dense_vector (1536 dims, knn indexed)",
  "last_updated": "date"
}
```

## Kafka Topics

| Topic | Producer | Consumer | Content |
|-------|---------|---------|---------|
| `raw-companies` | collector | enrichment | Raw company data from APIs |
| `enriched-companies` | enrichment | indexer | AI-enriched profiles with embeddings |
| `dlq-enrichment` | enrichment (on failure) | manual review | Failed enrichment payloads |

## API Endpoints (search-api)

```
GET  /companies/search?q=&industry=&country=&stage=&page=&size=
GET  /companies/{id}
GET  /companies/{id}/peers          # KNN vector similarity
GET  /companies/{id}/benchmark      # ES aggregations vs peer group
POST /companies/                    # manual add → triggers enrichment via Kafka
```

## Implementation Milestones

1. **Local infra** — docker-compose with ES, Kafka, Jaeger, Prometheus, Grafana
2. **Collector + Indexer** — seed data flows into ES, no AI yet
3. **Search API** — full-text search, filters, basic benchmarking aggregations
4. **AI Enrichment** — Claude enrichment + KNN peer discovery working end-to-end
5. **Observability** — OTel traces across all services visible in Jaeger
6. **Terraform** — deploy to AWS (tackled last, after local is solid)

## Scaling Decisions

- **Stateless services** → horizontal scaling via multiple instances
- **Kafka consumer groups** → add enrichment instances to scale throughput, zero config changes
- **ES bulk writes** → indexer batches 1000 docs per request, `refresh_interval=30s`
- **Redis cache-aside** → benchmark/peer results cached to spare ES aggregation load
- **ES alias pattern** → `companies` alias → `companies_v1` index; zero-downtime reindex by building v2 then flipping alias
- **Kafka partitions** → 12 partitions per topic; supports up to 12 parallel consumers per group
- **Idempotent writes** → ES upsert by `company_id`; safe to replay Kafka messages

## Development Commands

```bash
# Start all infrastructure locally
docker-compose up -d

# Run a service in dev mode
cd services/search-api && uvicorn main:app --reload

# View traces
open http://localhost:16686   # Jaeger UI

# View metrics
open http://localhost:3000    # Grafana

# Elasticsearch
open http://localhost:9200
```

## Environment Variables (per service)

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Elasticsearch
ES_HOST=localhost
ES_PORT=9200
ES_INDEX=companies

# Claude API
ANTHROPIC_API_KEY=...

# Redis
REDIS_URL=redis://localhost:6379

# OpenTelemetry
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_SERVICE_NAME=search-api  # change per service
```
