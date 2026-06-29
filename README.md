# AI LongList Builder — Competitive Intelligence Platform

An AI-powered competitive intelligence and peer benchmarking platform, built to discover companies, identify their market peers, and benchmark them on key metrics — at a target scale of 50M+ company records.

This project doubles as a hands-on learning ground for distributed systems: microservices, event streaming, search infrastructure, observability, and infrastructure as code.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Learning Goals](#learning-goals)
3. [Features](#features)
4. [Tech Stack](#tech-stack)
5. [Architecture](#architecture)
   - [High-Level Architecture](#high-level-architecture)
   - [Deep-Level Architecture](#deep-level-architecture)
6. [Microservices Breakdown](#microservices-breakdown)
7. [Data Model](#data-model)
8. [Kafka Topics](#kafka-topics)
9. [API Endpoints](#api-endpoints)
10. [Scaling & Optimization Decisions](#scaling--optimization-decisions)
11. [Observability](#observability)
12. [Project Structure](#project-structure)
13. [Frontend](#frontend)
14. [Development Setup](#development-setup)
15. [Environment Variables](#environment-variables)
16. [Implementation Roadmap](#implementation-roadmap)
17. [Status](#status)

---

## Introduction

Users can search for companies, discover their closest market peers using AI-driven semantic similarity, and benchmark them against each other on metrics like funding, headcount, and growth stage. The system is designed around an event-driven pipeline: data is ingested from external sources, enriched with AI-generated tags and embeddings, indexed into Elasticsearch, and served through a search API to a React frontend.

## Learning Goals

This project is intentionally designed to teach, hands-on:

- **Microservices** — independent services, each with a single responsibility and independent scaling
- **Kafka** — async, event-driven pipeline decoupling ingestion from processing
- **Elasticsearch** — full-text search + KNN vector similarity for peer discovery at scale
- **OpenTelemetry + Jaeger + Grafana** — distributed tracing and observability across services
- **Terraform** — infrastructure as code for AWS deployment (tackled last, after local is solid)

## Features

- **Company Search** — full-text search with filters (industry, country, funding stage)
- **Peer Discovery** — find semantically similar companies via vector (KNN) search, not just shared industry codes
- **Benchmarking** — compare a company's metrics (funding, headcount, growth) against its peer group using Elasticsearch aggregations
- **AI Enrichment** — Claude-generated tags, summaries, and embeddings per company
- **Manual Company Add** — submit a company manually, which triggers the same async enrichment pipeline
- **Observability Dashboards** — trace any request end-to-end across all services; live ingestion and search metrics

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Frontend | React + Vite + TypeScript | Fast dev, simple static build, no SSR complexity needed |
| Frontend data | TanStack Query | Server state + caching in the browser |
| Frontend styling | Tailwind + Recharts/Tremor | Utility-first styling, benchmark charts |
| Backend services | Python 3.12 + FastAPI | Fast, async, strong ES/Kafka client support |
| Message bus | Apache Kafka | Decouples services, replay-able, scales horizontally |
| Search & vector store | Elasticsearch 8.x | Full-text + KNN in one engine, handles 50M+ docs |
| AI enrichment | Claude API (claude-sonnet-4-6) | Tags, summaries, embeddings per company |
| Cache | Redis | Cache-aside for benchmarks, peer lists, hot searches |
| Tracing | OpenTelemetry → Jaeger | Distributed trace across all services |
| Metrics | Prometheus + Grafana | Ingestion rate, consumer lag, search latency |
| Infrastructure | Terraform + AWS | ECS Fargate, MSK, OpenSearch, ElastiCache |
| Containers | Docker + docker-compose | Local dev environment |

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          BROWSER                                 │
│                    React App (Vite)                             │
│         Search · Company Profiles · Peer Maps · Benchmarks      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ HTTPS
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                           CDN                                    │
│                   CloudFront / Vercel                           │
│          static assets cached at edge · API proxy               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       BACKEND SERVICES                           │
│                                                                  │
│   ┌─────────────┐     ┌──────────────┐     ┌────────────────┐  │
│   │  search-api │────►│     Redis    │────►│ Elasticsearch  │  │
│   │  (FastAPI)  │     │    (cache)   │     │   (search +    │  │
│   └─────────────┘     └──────────────┘     │  vector store) │  │
│                                             └────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               ▲
                               │ async pipeline
┌─────────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE                                │
│                                                                  │
│  [collector] ──► Kafka ──► [enrichment + Claude] ──► Kafka ──► [indexer]
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                               ▲
                               │
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
│          Crunchbase · Apollo · Public Datasets · CSVs           │
└─────────────────────────────────────────────────────────────────┘
```

### Deep-Level Architecture

```
BROWSER
  └─ React App (search, company profile, peer map, benchmark charts)
        │ HTTPS / REST JSON
        ▼
CDN (CloudFront / Vercel) — static assets at edge, /api/* forwarded
        │
        ▼
LOAD BALANCER — routes /api/* across N stateless search-api instances
        │
        ▼
search-api (FastAPI × N)
  ├── GET /search, /company/{id}, /peers/{id}, /benchmark/{id}
  ├── POST /companies/  (manual add → publishes to Kafka)
  │
  ├─► Redis (cache-aside: search 5m, peers 6h, benchmark 1h TTL)
  │       │ cache miss
  │       ▼
  └─► Elasticsearch Cluster
        ├── full-text + filters (search)
        ├── KNN dense_vector search (peers)
        └── aggregations: avg/p50/p90 (benchmark)

ASYNC PIPELINE
  collector × N → Kafka(raw-companies, 12 partitions) →
  enrichment × N (Claude: tags, summary, embedding) → Kafka(enriched-companies) →
  indexer × N (bulk upsert by company_id) → Elasticsearch
  failures → Kafka(dlq-enrichment) for manual review

OBSERVABILITY PLANE
  Every service → OpenTelemetry SDK → trace context via Kafka headers
  → Jaeger (traces) + Prometheus (metrics) → Grafana (dashboards)
```

## Microservices Breakdown

| Service | Responsibility | Scaling axis | Teaches |
|---|---|---|---|
| `collector` | Pulls company data from external APIs/CSVs, validates, deduplicates, publishes to Kafka | I/O bound — scale per data source | Kafka producer, rate limiting, circuit breakers |
| `enrichment` | Consumes raw companies, calls Claude API for tags/summary/embedding, publishes enriched data | CPU/cost bound — scale via consumer group | Kafka consumer groups, LLM integration |
| `indexer` | Consumes enriched companies, bulk-upserts into Elasticsearch | Write-throughput bound | ES bulk API, idempotent writes |
| `search-api` | Serves search, peer discovery, and benchmarking over REST | Read-latency bound — stateless, horizontally scaled | FastAPI, ES query DSL, KNN, aggregations |

## Data Model

Elasticsearch document mapping for the `companies` index:

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

All topics: 12 partitions, replication factor 3, partitioned by `company_id` for ordered per-company processing.

## API Endpoints

```
GET  /companies/search?q=&industry=&country=&stage=&page=&size=
GET  /companies/{id}
GET  /companies/{id}/peers          # KNN vector similarity
GET  /companies/{id}/benchmark      # ES aggregations vs peer group
POST /companies/                    # manual add → triggers enrichment via Kafka
```

## Scaling & Optimization Decisions

- **Stateless services** → horizontal scaling via multiple instances behind a load balancer
- **Kafka consumer groups** → add enrichment/indexer instances to scale throughput with zero config changes
- **ES bulk writes** → indexer batches 1000 docs per request, `refresh_interval=30s` to reduce write amplification
- **Redis cache-aside** → benchmark and peer results cached to spare Elasticsearch aggregation load
- **ES alias pattern** → `companies` alias → `companies_v1` index; zero-downtime reindex by building v2 then flipping the alias
- **Kafka partitioning** → 12 partitions per topic; supports up to 12 parallel consumers per group
- **Idempotent writes** → ES upsert by `company_id`; safe to replay Kafka messages without duplication
- **Dead Letter Queue** → failed enrichment payloads routed to `dlq-enrichment` instead of blocking the pipeline
- **No micro frontend** → a single React app is sufficient for one team; micro frontends would add complexity with no benefit at this scale

## Observability

- Every service is instrumented with the OpenTelemetry SDK
- Trace context propagates across Kafka via message headers, so a single trace spans collector → enrichment → indexer → search-api
- **Jaeger** visualizes distributed traces locally
- **Prometheus** scrapes metrics from each service's `/metrics` endpoint
- **Grafana** dashboards track: ingestion rate, Kafka consumer lag, ES indexing latency, search p50/p99 latency, Redis cache hit rate, DLQ size

## Project Structure

```
ai-longlist-builder/
├── services/
│   ├── collector/          # ingests from external APIs, publishes to Kafka
│   ├── enrichment/         # consumes raw-companies, calls Claude, publishes enriched-companies
│   ├── indexer/            # consumes enriched-companies, writes to Elasticsearch
│   └── search-api/         # FastAPI: search, peer discovery, benchmarking endpoints
├── frontend/                # React + Vite app
├── infrastructure/
│   └── terraform/          # AWS: ECS Fargate, MSK, OpenSearch, ElastiCache
├── docker-compose.yml      # local dev: ES, Kafka, Jaeger, Prometheus, Grafana
├── CLAUDE.md
└── README.md
```

## Frontend

A single React + Vite + TypeScript app (no micro frontend architecture — unnecessary at this scale and team size):

- `/search` — search bar, filters, results list
- `/company/:id` — company profile and metrics
- `/peers/:id` — peer map with similarity scores
- `/benchmark/:id` — charts comparing the company against its peer group

Talks to `search-api` over REST JSON, using TanStack Query for caching and server state. Deployed as a static build to a CDN (CloudFront or Vercel).

## Development Setup

```bash
# Start all infrastructure locally
docker-compose up -d

# Run a backend service in dev mode
cd services/search-api && uvicorn main:app --reload

# Run the frontend
cd frontend && npm run dev

# View traces
open http://localhost:16686   # Jaeger UI

# View metrics
open http://localhost:3000    # Grafana

# Elasticsearch
open http://localhost:9200
```

## Environment Variables

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

## Implementation Roadmap

1. **Local infra** — `docker-compose.yml` with Elasticsearch, Kafka, Jaeger, Prometheus, Grafana
2. **Collector + Indexer** — seed data flows into Elasticsearch, no AI yet
3. **Search API** — full-text search, filters, basic benchmarking aggregations
4. **AI Enrichment** — Claude enrichment + KNN peer discovery working end-to-end
5. **Frontend** — React app wired to search-api for search, peers, and benchmarks
6. **Observability** — OpenTelemetry traces across all services visible in Jaeger
7. **Terraform** — deploy to AWS (tackled last, after local is solid)

## Status

Architecture and system design are complete. The repository currently contains an empty FastAPI skeleton (`app/`) pending refactor into the `services/search-api/` structure described above. No services, pipeline, or frontend code have been implemented yet — next step is Milestone 1 (local infrastructure via `docker-compose.yml`).
