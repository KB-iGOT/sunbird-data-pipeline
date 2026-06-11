# Modern Data Pipeline Architecture
## From 100k TPS → 1M+ TPS: A Cloud-Native Rebuild

**Prepared for:** Karmayogi Bharat / iGOT Platform  
**Current Branch:** `cbrelease-4.8.31`  
**Date:** May 2026  
**Author:** christyfernandes

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current Architecture: What Works, What Doesn't](#2-current-architecture-what-works-what-doesnt)
3. [New Architecture: Overview](#3-new-architecture-overview)
4. [Technology Stack: Old vs New](#4-technology-stack-old-vs-new)
5. [Component Deep Dive](#5-component-deep-dive)
6. [Architecture Diagrams](#6-architecture-diagrams)
7. [Phase-by-Phase Implementation Plan](#7-phase-by-phase-implementation-plan)
8. [Infrastructure Cost Comparison](#8-infrastructure-cost-comparison)
9. [Real-Time Insights Layer](#9-real-time-insights-layer)
10. [Risk & Migration Strategy](#10-risk--migration-strategy)
11. [Success Metrics](#11-success-metrics)

---

## 1. Executive Summary

The current pipeline (built ~5 years ago) is a solid foundation — Kafka + Flink + Druid is a proven pattern. The optimization work on `cbrelease-4.8.31` pushed it from 64k to 100k+ TPS by fixing parallelism, async I/O, and eliminating redundant hops. But the ceiling is now the architecture itself, not configuration.

**Why the current design hits a hard ceiling:**
- Druid's ingestion model requires dedicated JVM-based Historical/MiddleManager nodes — expensive and operationally heavy at 10x scale
- SECOR is a 2014-era tool that writes Kafka → S3 line-by-line with no table semantics, schema evolution, or query capability
- Redis for deduplication at millions of events/sec becomes a single point of contention even with Bloom filters
- Flink 1.13.5 (2021) predates many performance improvements and the unified Table/SQL API maturation
- Kafka 2.4.0 is missing 4+ years of throughput, tiered storage, and KRaft improvements

**The new architecture targets:**

| Metric | Current (Optimized) | New Architecture |
|---|---|---|
| Throughput | ~100k TPS | 1M+ TPS |
| Hot-path Kafka hops | 2 | 1 |
| Real-time query latency | Druid: 1–5s | ClickHouse: <100ms |
| Infrastructure cost | High (Druid + SECOR + Redis) | ~40–60% lower |
| Archival | SECOR (line-by-line S3) | Iceberg (table format, queryable) |
| Insights | Dashboards query Druid | Live materialized views in ClickHouse |
| Dedup mechanism | Redis + Bloom | Kafka compaction + distributed Bloom |

---

## 2. Current Architecture: What Works, What Doesn't

### What Works

- **Kafka as the central nervous system** — correct choice, stays in the new architecture
- **Flink for stateful stream processing** — correct choice, upgraded not replaced
- **Bloom filter dedup** — excellent pattern, retained and scaled
- **Async enrichment (Lettuce + Caffeine)** — retained, extended
- **Kubernetes deployment** — retained, improved

### Pain Points at 10x Scale

#### Pain Point 1: Druid is Operationally Heavy
Druid requires: Coordinator, Broker, Router, Historical, MiddleManager, Overlord — 6 JVM services per cluster. At 1M TPS you need multiple Historical nodes with 100GB+ RAM to serve segments. The `druid.events.telemetry` ingestion is real-time but uses a pull-based model where MiddleManagers poll Kafka — creating lag and unpredictable ingestion rates under burst.

**Impact:** At 10x load, Druid MiddleManagers fall behind, query latency spikes, and scaling requires full JVM node provisioning (minutes, not seconds).

#### Pain Point 2: SECOR is a Dead-End Archival Tool
SECOR (Pinterest, 2014) writes Kafka → S3 as flat files. There is no schema, no partitioning by date/hour, no ACID guarantees. To query archived data, you must either:
- Load it all into Druid (expensive)
- Run Spark jobs over raw files (no query optimization)
- Parse JSON line by line in Python (unusable at scale)

**Impact:** You cannot run ad-hoc analytics on historical data cheaply. There's no way to backfill Druid from archives efficiently.

#### Pain Point 3: Redis Dedup at 1M TPS
At 1M TPS with Bloom filter reducing calls by 70%, you still have ~300k Redis `EXISTS` ops/sec hitting a single Redis instance. Redis is single-threaded for write operations. A single Redis node tops out at ~300–500k ops/sec. At 1M TPS, this becomes the wall.

**Impact:** Cannot scale dedup beyond current Redis capacity without Redis Cluster (complex) or architectural change.

#### Pain Point 4: No Real-Time Insights
Druid serves analytical queries but with 1–5 second latency minimum. There is no push-based real-time view — dashboards must poll. No stream-time aggregations, no per-minute rollups without full Druid materialized view jobs.

**Impact:** Operations team cannot see "last 60 seconds" metrics. No real-time alerting on event anomalies.

#### Pain Point 5: Schema is Implicit JSON
Events are JSON with no formal schema contract. A malformed event from a new SDK version passes validation if it has the right top-level keys, but downstream jobs fail silently on missing nested fields. SECOR archives the broken events. They are essentially unrecoverable without re-processing.

**Impact:** Schema drift causes silent data quality issues that only surface in Druid query results weeks later.

---

## 3. New Architecture: Overview

The new architecture rests on three principles:

1. **One hop in the hot path** — events go from ingest → processing → storage in a single Kafka message. No intermediate topics.
2. **Storage is the query engine** — ClickHouse ingests directly from Kafka and serves queries on the same data, sub-second. No separate ingestion job needed.
3. **Archive is queryable from day one** — Apache Iceberg on object storage gives full SQL access to all historical data at near-zero cost.

### Technology Pillars

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         NEW ARCHITECTURE PILLARS                            │
├─────────────┬──────────────────┬──────────────────┬──────────────────────────┤
│  INGEST     │  PROCESS         │  STORE           │  SERVE                   │
│             │                  │                  │                          │
│  Redpanda   │  Apache Flink    │  ClickHouse      │  Apache Superset         │
│  (Kafka-    │  2.0 + Flink SQL │  (Real-time      │  (Business dashboards)   │
│  compatible │                  │   OLAP)          │                          │
│  C++ broker)│  Apache Avro     │                  │  Grafana                 │
│             │  (Schema)        │  Apache Iceberg  │  (Ops dashboards)        │
│             │                  │  on S3/GCS       │                          │
│             │  DragonflyDB     │  (Data Lake,     │  Trino / Spark SQL       │
│             │  (Cache + Dedup) │   ML input)      │  (Ad-hoc queries)        │
└─────────────┴──────────────────┴──────────────────┴──────────────────────────┘
```

---

## 4. Technology Stack: Old vs New

### Full Comparison Table

| Layer | Current | New | Why Change |
|---|---|---|---|
| **Message Broker** | Apache Kafka 2.4.0 + ZooKeeper | **Redpanda 24.x** | C++ vs JVM: 10x throughput per node, no ZooKeeper, tiered storage built-in, Kafka-compatible |
| **Stream Processing** | Apache Flink 1.13.5 (Scala 2.12) | **Apache Flink 2.0** (Scala 2.13 / Java 17) | Unified Table API, Flink SQL, better RocksDB state, improved watermarking, 30–50% throughput gain |
| **Schema Contract** | Implicit JSON (no schema) | **Apache Avro + Schema Registry** | Compile-time schema validation, backward/forward compatibility enforced, eliminates silent data quality issues |
| **Deduplication** | Redis + Guava Bloom Filter | **DragonflyDB + Distributed Bloom** | DragonflyDB: Redis-compatible, multi-threaded, 25x more throughput than Redis single node |
| **In-process Cache** | Caffeine (50k entries, 5-min TTL) | **Caffeine** (unchanged) | Already optimal, keep |
| **Analytics DB** | Apache Druid (6-service cluster) | **ClickHouse** (3-node cluster) | 1000x faster queries, real-time ingestion from Kafka natively, 60% lower infra cost |
| **Archival** | SECOR → flat files on S3 | **Apache Iceberg on S3/GCS** | Table format: ACID, schema evolution, time-travel, queryable by Spark/Trino/Flink directly |
| **Enrichment Store** | Redis (Jedis/Lettuce HGETALL) | **DragonflyDB** (same Lettuce client) | Same API, higher throughput, multi-threaded reads |
| **Real-time Insights** | Grafana polling Druid | **ClickHouse Materialized Views + Superset** | Sub-100ms latency, push-based updates, no polling |
| **Batch Historical Queries** | Druid deep storage | **Trino on Iceberg** | Standard SQL, federation across Iceberg + ClickHouse, no Druid Historical nodes |
| **Deployment** | Kubernetes + Ansible | **Kubernetes + Helm** (Ansible kept for infra provisioning) | Helm is idiomatic for Flink/ClickHouse/Redpanda operators |
| **Observability** | Prometheus + Grafana | **OpenTelemetry + Prometheus + Grafana** | Distributed tracing across Flink jobs, correlation IDs in events |

### Why Redpanda over Kafka Upgrade

| Attribute | Kafka 3.7 (KRaft) | Redpanda 24.x |
|---|---|---|
| Implementation | JVM (Java) | C++ (Seastar framework) |
| ZooKeeper | Eliminated in 3.7 | Never needed |
| Throughput / node | ~300–500 MB/s | ~1.5–2 GB/s |
| Latency (p99) | 10–50ms | 1–5ms |
| Tiered storage | Add-on (Confluent) | Built-in, open source |
| Schema registry | Separate service | Built-in |
| Kafka compatibility | Native | 100% wire-compatible |
| Ops complexity | Medium | Low |

Redpanda is a drop-in replacement: **zero SDK changes required**. Your existing Kafka producers (mobile SDK, web SDK) and consumers (Flink jobs) work unchanged.

### Why ClickHouse over Druid

| Attribute | Apache Druid | ClickHouse |
|---|---|---|
| Query latency (p50) | 1–5 seconds | 10–100 ms |
| Real-time ingestion | Pull-based (Kafka polling) | Direct Kafka consumer (native) |
| Cluster nodes | 6 JVM services minimum | 3 nodes (or 1 for dev) |
| SQL support | Limited (Druid SQL) | Full SQL + extensions |
| Materialized views | Limited, complex | Native, fast, incremental |
| RAM requirement | Very high (segment cache) | High (columnar cache) |
| Cost at scale | $$$ | $$ |
| Data format | Druid segments (proprietary) | Columnar (MergeTree) |
| ML integration | None | DataFrames via SQL |
| Community | Declining | Fast-growing |

**Note:** ClickHouse setup has already begun in this repo (see recent commits adding SSL, macros, and user configuration). The new architecture formalizes this as the primary analytics store.

---

## 5. Component Deep Dive

### 5.1 Redpanda (Message Broker)

**Role:** Receives all events from SDKs and passes them between processing stages.

**Key improvements over current Kafka:**
- **Tiered storage:** Hot data (last 7 days) on NVMe SSD. Cold data automatically offloaded to S3/GCS at 1/10th the cost. No need for SECOR — Redpanda handles its own archival.
- **Built-in Schema Registry:** Avro schemas registered once, enforced at produce time. Malformed events rejected at the broker, not silently passed to Flink.
- **No ZooKeeper:** 3 fewer nodes per cluster. Simpler operations.
- **Console UI:** Built-in web UI for topic inspection, consumer lag monitoring, schema browsing.

**Cluster sizing for 1M TPS:**
```
3 × r6i.4xlarge (16 vCPU, 128 GB RAM, 2TB NVMe)
= ~1.5M events/sec sustained
Tiered storage: S3 for anything older than 7 days (near-zero cost)
```

**Topic structure (simplified from current 16 topics → 6):**

| Topic | Purpose | Retention |
|---|---|---|
| `{env}.telemetry.ingest` | Raw SDK batches | 24 hours (hot) |
| `{env}.telemetry.processed` | Validated, deduplicated, enriched events | 7 days (hot) |
| `{env}.telemetry.dlq` | Dead letter: schema failures, oversized | 30 days |
| `{env}.telemetry.audit` | AUDIT events (user-cache updates) | 7 days |
| `{env}.druid.events.telemetry` | ClickHouse ingestion feed | 2 days |
| `{env}.druid.events.summary` | ClickHouse summary feed | 2 days |

**Reduction: 16 topics → 6 active topics in hot path.**

---

### 5.2 Apache Flink 2.0 (Stream Processing)

**Role:** Single unified processing job — ingest, validate, deduplicate, enrich, route.

**Key improvements:**

1. **Flink SQL for routing logic:** Replace Scala boilerplate with declarative SQL views. A route that previously required a `ProcessFunction` with a side output becomes a `WHERE eid = 'ME_WORKFLOW_SUMMARY'` filter in SQL.

2. **Unified streaming + batch API:** Run the same job in bounded mode to reprocess historical Iceberg data (backfills). Critical for fixing past data quality issues.

3. **Improved RocksDB state backend:** In-memory hot state, SSD-backed cold state. Enables much larger state (e.g., 10M Bloom filter entries per TaskManager) without OOM.

4. **Adaptive batch scheduling:** Flink 2.0 automatically adjusts parallelism per operator based on actual throughput. No manual `parallelism.default` tuning needed in steady state.

**Processing pipeline (single Flink job, two stages):**

```
Stage 1: TelemetryIntakeJob
  ├── Read: telemetry.ingest (Avro-decoded at broker)
  ├── Validate: schema check (no-op — Redpanda already rejected bad events)
  ├── Dedup: DragonflyDB Bloom + SET NX with TTL
  ├── Unpack: batch → individual events (same as current extractor)
  ├── Route: audit / error / log / unique
  └── Write: telemetry.processed + side topics

Stage 2: EnrichmentJob
  ├── Read: telemetry.processed
  ├── Enrich: async DragonflyDB (Lettuce, 500 in-flight)
  │          Caffeine L1 cache (50k entries, 5-min TTL)
  ├── Skip: LOG / ERROR / AUDIT events (same as P2.4)
  ├── Route: ME_WORKFLOW_SUMMARY → druid.events.summary
  │          everything else → druid.events.telemetry
  └── Write: Iceberg (data lake) + druid topics (ClickHouse feed)
```

**Reduction: 4 Flink jobs → 2 Flink jobs. Hot-path Kafka hops: 2 → 1.**

---

### 5.3 Apache Avro + Schema Registry

**Role:** Formal contract for event structure between producers and consumers.

**How it works:**
- Schemas stored in Redpanda's built-in Schema Registry
- Producers (SDK, Flink jobs) serialize to Avro
- Redpanda validates schema at produce time — malformed events never enter the pipeline
- Schema evolution: add optional fields without breaking existing consumers (backward compatibility)

**Example schema for a telemetry event:**
```json
{
  "type": "record",
  "name": "TelemetryEvent",
  "namespace": "org.sunbird.telemetry",
  "fields": [
    {"name": "eid",     "type": "string"},
    {"name": "ets",     "type": "long"},
    {"name": "ver",     "type": "string"},
    {"name": "mid",     "type": "string"},
    {"name": "actor",   "type": {"type": "record", "name": "Actor", "fields": [
      {"name": "id",   "type": "string"},
      {"name": "type", "type": "string"}
    ]}},
    {"name": "context", "type": "..."},
    {"name": "edata",   "type": "..."}
  ]
}
```

**Impact:** Eliminates the entire `schema validation` step in `pipeline-preprocessor` — the broker enforces it. Flink jobs receive only valid events.

---

### 5.4 DragonflyDB (Cache + Deduplication)

**Role:** Replaces Redis for both enrichment cache and deduplication.

**Why DragonflyDB:**
- Redis-compatible: same Lettuce/Jedis client, zero code changes
- Multi-threaded (uses all CPU cores): 25x more throughput per node vs Redis
- At 1M TPS with 70% Bloom hit rate: ~300k remaining dedup ops/sec handled easily by a single DragonflyDB node
- At 2M TPS: single DragonflyDB node still has headroom

**Dedup strategy at 1M TPS:**
```
isUniqueEvent(mid):
  L1: Caffeine per-TaskManager Bloom (10M entries, ~24MB) → definitely unique? return true
  L2: DragonflyDB SET NX with TTL (atomic, no EXIST → SET race) → unique? mark + return true
                                                                  → duplicate? return false

Key format: "dup:{mid}"
TTL: 7 days (matches Kafka retention, prevents false positives on replay)
```

**Improvement over current:** `SET NX` (atomic) replaces the two-step `EXISTS` + `SET`, halving round-trips and eliminating the race condition where two concurrent Flink slots could both see a key as new.

---

### 5.5 Apache Iceberg (Data Lake)

**Role:** Replaces SECOR for archival. Every processed event is written to Iceberg tables on S3/GCS.

**Why Iceberg over SECOR:**

| Capability | SECOR | Apache Iceberg |
|---|---|---|
| Format | Flat JSON files | Parquet/ORC columnar (10–100x smaller) |
| Schema | None | Versioned, with evolution |
| Query | Must load into Druid or parse JSON | Trino/Spark SQL directly, seconds |
| Partitioning | By date (fixed) | By any field, hidden partitioning |
| ACID | No | Yes (snapshot isolation) |
| Time-travel | No | Yes (query any snapshot) |
| Backfill | Re-run SECOR job (painful) | Flink batch job on Iceberg |
| Cost | S3 storage | S3 storage (same, but compressed: 5–10x less) |

**Table structure:**
```sql
-- Primary events table (all events)
CREATE TABLE telemetry.events (
    eid        STRING,
    ets        BIGINT,
    mid        STRING,
    actor_id   STRING,
    actor_type STRING,
    context_*  STRING,
    edata      STRING,   -- JSON blob for flexible edata
    -- enrichment fields
    user_*     STRING,
    device_*   STRING,
    content_*  STRING,
    location_* STRING,
    -- metadata
    processed_at TIMESTAMP,
    partition_date DATE
)
PARTITIONED BY (days(processed_at), eid)
STORED AS PARQUET;
```

**Flink writes to Iceberg using the official `flink-iceberg` connector** — same Flink job, extra sink alongside the ClickHouse Kafka topic write.

---

### 5.6 ClickHouse (Real-Time Analytics)

**Role:** Primary query engine for dashboards, reports, and real-time insights.

**Ingestion model:** ClickHouse reads directly from Redpanda using its native `Kafka` engine table type. No separate ingestion job:

```sql
-- ClickHouse reads directly from Redpanda/Kafka
CREATE TABLE telemetry_queue (
    eid String,
    ets UInt64,
    mid String,
    actor_id String,
    content_id String,
    session_id String,
    device_id String,
    user_id String,
    edata String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = '{env}.druid.events.telemetry',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'Avro',
    kafka_num_consumers = 8;  -- parallel consumers

-- Events land in the main table via materialized view
CREATE MATERIALIZED VIEW telemetry_mv TO telemetry AS
SELECT * FROM telemetry_queue;
```

**Real-time aggregations via materialized views:**

```sql
-- Pre-aggregated: events per content per hour
CREATE MATERIALIZED VIEW events_per_content_hourly
ENGINE = SummingMergeTree()
PARTITION BY toDate(hour)
ORDER BY (content_id, hour)
AS SELECT
    content_id,
    toStartOfHour(toDateTime(ets / 1000)) AS hour,
    count() AS event_count,
    uniqExact(actor_id) AS unique_users
FROM telemetry
GROUP BY content_id, hour;
```

This view **updates in real time** as events arrive. A Superset dashboard querying `events_per_content_hourly` returns in <10ms for any time range.

**Cluster sizing for 1M TPS ingestion:**
```
3 × r6i.8xlarge (32 vCPU, 256 GB RAM, 3.8TB NVMe SSD)
Replication factor: 2
Expected query p50: <50ms, p99: <500ms
```

---

### 5.7 Apache Superset (Business Intelligence)

**Role:** Self-service dashboards on ClickHouse data.

**Capabilities:**
- Drag-and-drop chart builder (no SQL required for standard reports)
- Native ClickHouse connector
- Real-time refresh (polls ClickHouse materialized views, effectively real-time)
- Row-level security (user sees only their org's data)
- Dashboard embedding (iframe-embeddable into Karmayogi portal)
- Alerts: email/Slack when metric crosses threshold

**Sample dashboards:**
1. **Platform Health Dashboard:** Events/sec last 60s, error rate, dedup rate — refreshes every 10 seconds
2. **Content Engagement:** Top content by plays, completions, time-spent — real-time with last-30-min window
3. **User Journey Funnel:** START → INTERACT → END completion rates by course/content
4. **Geographic Distribution:** State-wise active users map — updated hourly from Iceberg

---

## 6. Architecture Diagrams

### 6.1 New Architecture: Full Flow

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                              SDK / Telemetry API Layer                              │
│         Mobile SDK  │  Web (Portal) SDK  │  Desktop App  │  Batch Upload API        │
└──────────────────────────────────┬─────────────────────────────────────────────────┘
                                   │  HTTPS / gRPC
                                   ▼
                    ┌──────────────────────────────┐
                    │  API Gateway (Kong / Nginx)  │
                    │  Rate limiting · Auth · TLS  │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │         REDPANDA CLUSTER             │
                    │  3-node · Kafka-compatible · KRaft   │
                    │  Built-in Schema Registry (Avro)     │
                    │  Tiered Storage → S3/GCS (cold)      │
                    │                                      │
                    │  Topics:                             │
                    │  ├── {env}.telemetry.ingest          │ ← SDK writes here
                    │  ├── {env}.telemetry.processed       │ ← Stage 1 output
                    │  ├── {env}.telemetry.audit           │ ← Audit events
                    │  ├── {env}.telemetry.dlq             │ ← Failed events
                    │  ├── {env}.druid.events.telemetry    │ ← ClickHouse input
                    │  └── {env}.druid.events.summary      │ ← ClickHouse summary
                    └──────────────┬───────────────────────┘
                                   │
               ┌───────────────────┴──────────────────────┐
               │                                          │
    ┌──────────▼────────────────────────────────────────┐ │
    │      FLINK JOB 1: TelemetryIntakeJob              │ │
    │   Java 17 · Flink 2.0 · Parallelism: 8–32        │ │
    │                                                   │ │
    │   ┌──────────────────────────────────────────┐   │ │
    │   │  Source: telemetry.ingest (Avro)         │   │ │
    │   │  ↓                                       │   │ │
    │   │  Unpack batches → individual events      │   │ │
    │   │  ↓                                       │   │ │
    │   │  Dedup: Caffeine L1 Bloom                │   │ │
    │   │       + DragonflyDB SET NX               │   │ │
    │   │  ↓                                       │   │ │
    │   │  Route by eid type                       │   │ │
    │   └──────────────────────────────────────────┘   │ │
    │                                                   │ │
    │   Sinks:                                          │ │
    │   ├── telemetry.processed (unique events)         │ │
    │   ├── telemetry.audit (AUDIT events)              │ │
    │   └── telemetry.dlq (duplicates, failures)        │ │
    └──────────────────────────────────────────────────-┘ │
                                                          │
    ┌──────────────────────────────────────────────────┐  │
    │      FLINK JOB 2: EnrichmentJob                  │  │
    │   Java 17 · Flink 2.0 · Parallelism: 8–32       │  │
    │   AsyncWaitOperator: 500 in-flight               │  │
    │                                                   │  │
    │   ┌──────────────────────────────────────────┐   │  │
    │   │  Source: telemetry.processed             │   │  │
    │   │  ↓                                       │   │  │
    │   │  Skip: LOG/ERROR/AUDIT → pass-through    │   │  │
    │   │  ↓ (for enrichable events)               │   │  │
    │   │  Caffeine L1 cache (50k, 5-min TTL)      │   │  │
    │   │  DragonflyDB async (Lettuce, all 4       │   │  │
    │   │    stores concurrent, 500 in-flight)      │   │  │
    │   │  ↓                                       │   │  │
    │   │  Route: ME_WORKFLOW_SUMMARY → summary    │   │  │
    │   │         everything else → telemetry      │   │  │
    │   └──────────────────────────────────────────┘   │  │
    │                                                   │  │
    │   Sinks:                                          │  │
    │   ├── druid.events.telemetry (ClickHouse feed)    │  │
    │   ├── druid.events.summary (ClickHouse feed)      │  │
    │   └── Iceberg table (S3/GCS — data lake)          │  │
    └───────────────────────────────────────────────────┘  │
                                                           │
               ┌───────────────────────────────────────────┘
               │     (parallel, independent)
               │
    ┌──────────▼──────────────────────────────────────────────────────────────────┐
    │                           STORAGE LAYER                                     │
    │                                                                             │
    │  ┌────────────────────────────┐   ┌───────────────────────────────────┐    │
    │  │  CLICKHOUSE CLUSTER        │   │  APACHE ICEBERG on S3/GCS         │    │
    │  │  3-node · MergeTree        │   │  (Parquet · ACID · Queryable)     │    │
    │  │                            │   │                                   │    │
    │  │  Kafka Engine →            │   │  telemetry/events/               │    │
    │  │    telemetry (MergeTree)   │   │    partitioned by date + eid     │    │
    │  │  Materialized views for    │   │                                   │    │
    │  │    real-time aggregations  │   │  Queried by: Trino · Spark SQL   │    │
    │  │                            │   │  for ad-hoc / ML / backfill      │    │
    │  │  Replicated · HA           │   │                                   │    │
    │  └────────────┬───────────────┘   └───────────────────────────────────┘    │
    │               │                                                             │
    │  ┌────────────▼──────────────────────────────────────────────────────┐     │
    │  │              DRAGONFLY DB (cache + dedup store)                   │     │
    │  │   Redis-compatible · multi-threaded · 25x throughput vs Redis     │     │
    │  │   Stores: content · user · device · location · dialcode · dedup  │     │
    │  └───────────────────────────────────────────────────────────────────┘     │
    └─────────────────────────────────────────────────────────────────────────────┘
                                         │
    ┌────────────────────────────────────▼───────────────────────────────────────┐
    │                           INSIGHTS LAYER                                   │
    │                                                                             │
    │  ┌──────────────────────┐   ┌──────────────────┐   ┌──────────────────┐   │
    │  │  APACHE SUPERSET     │   │     GRAFANA       │   │  TRINO (ad-hoc)  │   │
    │  │  Business dashboards │   │  Ops / SLA /      │   │  SQL queries on  │   │
    │  │  on ClickHouse data  │   │  Infra metrics    │   │  Iceberg lake    │   │
    │  │  Real-time refresh   │   │  Flink job stats  │   │  federation      │   │
    │  │  Embeddable iframes  │   │  Kafka lag alerts │   │  across stores   │   │
    │  └──────────────────────┘   └──────────────────┘   └──────────────────┘   │
    └────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Data Flow: Current vs New (Side-by-Side)

```
CURRENT (Optimized)                    NEW ARCHITECTURE
═══════════════════                    ════════════════

[telemetry.ingest]                     [telemetry.ingest]
        │                                      │
        ▼                                      ▼
┌───────────────────┐               ┌──────────────────────┐
│ telemetry-intake  │               │ TelemetryIntakeJob   │
│ (Flink 1.13, JVM) │               │ (Flink 2.0, Java 17) │
│ Merged extractor  │               │ Avro decode (free —  │
│ + preprocessor    │               │   Redpanda enforces) │
│ Guava Bloom +     │               │ Caffeine + Dragonfly │
│ Redis dedup       │               │ SET NX dedup         │
└────────┬──────────┘               └──────────┬───────────┘
         │                                     │
   ┌─────┴──────┐                        ┌─────┴──────┐
   │ telemetry  │                        │ telemetry  │
   │  .unique   │  ← 1 Kafka hop         │ .processed │  ← 1 Kafka hop
   └─────┬──────┘                        └─────┬──────┘
         │                                     │
         ▼                                     ▼
┌────────────────────┐              ┌──────────────────────┐
│  de-normalization  │              │  EnrichmentJob        │
│  (Flink 1.13)      │              │  (Flink 2.0)          │
│  Async Lettuce     │              │  Async Lettuce        │
│  Caffeine cache    │              │  Caffeine cache       │
│  + Redis           │              │  + DragonflyDB        │
└────────┬───────────┘              └──────────┬────────────┘
         │                                     │
    ┌────┴─────┐                         ┌─────┴──────────────────────┐
    │druid.ev..│ ← 1 more Kafka hop      │ druid.events.telemetry     │
    └────┬─────┘ (2 total in hot path)   │ druid.events.summary       │
         │                               │ [+ Iceberg sink]           │
         ▼                               └─────┬──────────────────────┘
    [Apache Druid]                             │           │
    6-service cluster                          ▼           ▼
    High RAM · slow ingest         [ClickHouse]      [Iceberg S3]
    1–5s query latency             3-node cluster    Parquet format
                                   <100ms queries    SQL queryable
                                   Native Kafka      Time-travel
                                   real-time feed    ACID + schema

SECOR → flat S3 files              Iceberg → columnar Parquet
No schema, no query               Full SQL via Trino/Spark

Grafana polls Druid               Superset on ClickHouse MVs
5-10s refresh minimum             <1s refresh, real-time
```

### 6.3 Kafka Hops: Past vs Present vs Future

```
Original (5 years ago):     ingest → raw → unique → denorm → druid   = 4 hops
Optimized (current):        ingest → unique → druid                   = 2 hops
New Architecture:           ingest → processed → druid                = 2 hops*

*Same hop count, but:
  - Each hop is 10x faster (Redpanda vs Kafka)
  - ClickHouse reads directly from Kafka (no Druid ingestion job)
  - Iceberg write is a parallel sink (no extra hop)
  - Schema validation moved to broker (no Flink cycle for bad events)
```

---

## 7. Phase-by-Phase Implementation Plan

### Overview

```
Phase 0: Foundation         Weeks 1–4     Infrastructure setup
Phase 1: Schema Contract    Weeks 5–8     Avro + Schema Registry
Phase 2: ClickHouse MVP     Weeks 9–14    Analytics on ClickHouse
Phase 3: Iceberg Lake       Weeks 15–20   Replace SECOR
Phase 4: Flink 2.0          Weeks 21–28   Processing upgrade
Phase 5: Redpanda           Weeks 29–34   Broker replacement
Phase 6: Real-time Insights Weeks 35–40   Superset + MVs
Phase 7: Decommission       Weeks 41–48   Remove Druid, SECOR, Redis
```

Each phase is independently deployable. The pipeline remains 100% operational throughout — no big-bang cutover.

---

### Phase 0: Foundation (Weeks 1–4)

**Goal:** Prepare the new infrastructure alongside existing. Nothing is replaced yet.

**Tasks:**

| Task | Tool | Notes |
|---|---|---|
| Provision ClickHouse cluster | Kubernetes + ClickHouse Operator | Already started (SSL, macros, users done in recent commits) |
| Configure ClickHouse replication | ZooKeeper / ClickHouse Keeper | 3-node replica set |
| Deploy DragonflyDB | Kubernetes (replace Redis) | Redis-compatible, zero client changes |
| Set up Apache Superset | Kubernetes Helm chart | Connect to ClickHouse |
| Set up Trino | Kubernetes | For Iceberg queries (later) |
| Provision Redpanda in staging | Kubernetes | Test alongside existing Kafka |
| Set up OpenTelemetry collector | Kubernetes DaemonSet | Tracing backbone |

**Output:** New infra running, existing pipeline unchanged.

**Validation:**
- ClickHouse health check via HTTP
- DragonflyDB responds to Redis `PING`
- Superset shows "ClickHouse connected"
- Redpanda console UI accessible

---

### Phase 1: Schema Contract (Weeks 5–8)

**Goal:** Define Avro schemas for all telemetry event types. No pipeline changes yet.

**Tasks:**

| Task | Details |
|---|---|
| Define Avro schemas for all `eid` types | START, END, INTERACT, ASSESS, RESPONSE, ERROR, LOG, AUDIT, IMPRESSION, SHARE, SEARCH, FEEDBACK, ME_WORKFLOW_SUMMARY |
| Register schemas in Redpanda Schema Registry | `avro-tools` or Redpanda Console |
| Write schema validation test harness | Read real events from `telemetry.raw` in staging, validate against schemas |
| Document schema evolution rules | Which fields are required vs optional, backward/forward compat rules |
| Update SDK documentation | Mobile SDK, web SDK — Avro encoding instructions |

**Output:** Schemas defined and validated against 1M real events from staging.

**Why this first:** Schema is the hardest cross-team dependency. Defining it before code changes means SDK teams have clear spec. Once defined, Redpanda enforces it — no Flink change needed.

---

### Phase 2: ClickHouse as Analytics Backend (Weeks 9–14)

**Goal:** ClickHouse serving real dashboards in production, Druid still running in parallel.

**Sub-tasks:**

#### 2a: Create ClickHouse tables and ingest from existing Kafka

```sql
-- Raw telemetry table
CREATE TABLE IF NOT EXISTS telemetry (
    eid         LowCardinality(String),
    ets         UInt64,
    mid         String,
    actor_id    String,
    actor_type  LowCardinality(String),
    session_id  String,
    channel     LowCardinality(String),
    env         LowCardinality(String),
    app_id      String,
    content_id  String,
    content_ver String,
    device_id   String,
    user_id     String,
    -- location
    state       LowCardinality(String),
    district    LowCardinality(String),
    -- edata (flexible)
    edata       String,
    -- metadata
    indexed_at  DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/telemetry', '{replica}')
PARTITION BY (toYYYYMM(toDateTime(ets / 1000)))
ORDER BY (channel, eid, toDateTime(ets / 1000))
SETTINGS index_granularity = 8192;

-- Kafka engine (reads from existing druid.events.telemetry topic)
CREATE TABLE telemetry_kafka_source (
    raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{existing-kafka}:9092',
    kafka_topic_list = '{env}.druid.events.telemetry',
    kafka_group_name = 'clickhouse-ingest-v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 4;
```

#### 2b: Key materialized views

```sql
-- Sessions per hour
CREATE MATERIALIZED VIEW sessions_hourly
ENGINE = SummingMergeTree ORDER BY (channel, hour)
AS SELECT
    channel,
    toStartOfHour(toDateTime(ets/1000)) AS hour,
    count() AS session_count,
    uniqExact(session_id) AS unique_sessions,
    uniqExact(actor_id) AS unique_users
FROM telemetry WHERE eid = 'START'
GROUP BY channel, hour;

-- Content completion funnel
CREATE MATERIALIZED VIEW content_funnel_hourly
ENGINE = SummingMergeTree ORDER BY (content_id, eid, hour)
AS SELECT
    content_id,
    eid,
    toStartOfHour(toDateTime(ets/1000)) AS hour,
    count() AS event_count,
    uniqExact(actor_id) AS unique_users
FROM telemetry WHERE eid IN ('START', 'END', 'INTERACT')
GROUP BY content_id, eid, hour;
```

#### 2c: Validate ClickHouse vs Druid parity

Run both Druid and ClickHouse in parallel for 4 weeks. Compare daily event counts and user counts — must match within 0.01%.

**Output:** ClickHouse serving staging dashboards. Druid still primary for production.

---

### Phase 3: Iceberg Data Lake (Weeks 15–20)

**Goal:** All new events written to Iceberg on S3/GCS. SECOR continues running in shadow.

**Tasks:**

#### 3a: Set up Iceberg catalog

```yaml
# Hive Metastore or AWS Glue as Iceberg catalog
iceberg:
  catalog:
    type: hive       # or glue, rest
    uri: thrift://metastore:9083
  warehouse: s3://your-bucket/iceberg-warehouse/
```

#### 3b: Add Iceberg sink to existing de-normalization Flink job

```scala
// Add alongside existing Kafka sink in DenormalizationStreamTask.scala
val icebergSink = FlinkSink
  .forRowData(denormStream.map(_.toRow()))
  .table(catalog.loadTable(TableIdentifier.of("telemetry", "events")))
  .tableLoader(tableLoader)
  .upsert(false)
  .build()

denormStream.addSink(icebergSink)
  .name("iceberg-events-sink")
  .uid("iceberg-events-sink")
```

#### 3c: Set up Trino for Iceberg queries

```sql
-- Trino query on Iceberg (no data movement, reads Parquet directly)
SELECT
    date_trunc('day', from_unixtime(ets/1000)) AS date,
    eid,
    count(*) AS event_count
FROM iceberg.telemetry.events
WHERE processed_at >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

#### 3d: Validate Iceberg completeness vs SECOR

Compare daily row counts from SECOR S3 files vs Iceberg. Must match.

**Output:** Iceberg receiving all events. SECOR running in parallel (shadow).

---

### Phase 4: Flink 2.0 Upgrade (Weeks 21–28)

**Goal:** Replace Flink 1.13.5 jobs with Flink 2.0 jobs. No pipeline change — same logic, better runtime.

**Tasks:**

#### 4a: Upgrade `dp-core` to Java 17 + Flink 2.0

```xml
<!-- pom.xml changes -->
<flink.version>2.0.0</flink.version>
<java.version>17</java.version>
<!-- Remove: scala.version — migrate to Java-native API -->
```

**Key API changes Flink 1.13 → 2.0:**
- `StreamExecutionEnvironment.getExecutionEnvironment()` → unchanged
- `DataStream<T>` API → unchanged (backward compatible)
- `ProcessFunction` → unchanged
- `FlinkKafkaConsumer` → `KafkaSource` (new Source API, better parallelism)
- `FlinkKafkaProducer` → `KafkaSink` (new Sink API, exactly-once)
- `AsyncFunction` → unchanged

#### 4b: Migrate from Scala to Java

Flink 2.0 deprecates the Scala API. Migrate each job one at a time:
- `DenormalizationAsyncFunction.scala` → `DenormalizationAsyncFunction.java`
- `PipelinePreprocessorFunction.scala` → `PipelinePreprocessorFunction.java`
- `TelemetryIntakeStreamTask.scala` → `TelemetryIntakeStreamTask.java`

The logic is identical — only syntax changes.

#### 4c: Adopt RocksDB state backend

```java
// Enables incremental checkpointing — critical for large state (Bloom filters)
env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // true = incremental
env.getCheckpointConfig().setCheckpointInterval(30_000);
env.getCheckpointConfig().setCheckpointStorage("s3://your-bucket/flink-checkpoints/");
```

#### 4d: Enable Adaptive Parallelism

```java
// Flink 2.0: auto-tunes parallelism per operator based on observed throughput
env.getConfig().setAdaptiveSplittingEnabled(true);
// Remove hardcoded parallelism.default = 4 — Flink will tune it
```

**Validation:** Run old jobs and new jobs in parallel for 1 week on staging. Compare output to `telemetry.processed` topic — byte-for-byte identical events.

**Output:** All jobs on Flink 2.0, Java 17. Flink 1.13 cluster decommissioned.

---

### Phase 5: Redpanda Migration (Weeks 29–34)

**Goal:** Replace Kafka with Redpanda. Zero downtime, zero SDK changes.

**Tasks:**

#### 5a: Deploy Redpanda cluster

```yaml
# Redpanda Helm chart values
redpanda:
  statefulset:
    replicas: 3
  storage:
    tieredStorageEnabled: true
    tieredStorageCredentials:
      secretName: gcs-redpanda-credentials
  config:
    cluster:
      auto_create_topics_enabled: false
      default_topic_replications: 3
```

#### 5b: Mirror existing Kafka → Redpanda (MirrorMaker 2)

```yaml
# Kafka MirrorMaker 2 config
source.cluster.alias: kafka-prod
target.cluster.alias: redpanda-prod
topics: .*telemetry.*,.*druid.*
sync.topic.configs.enabled: true
replication.factor: 3
```

#### 5c: Cut over consumers one at a time

1. Point ClickHouse Kafka engine → Redpanda
2. Point Flink EnrichmentJob → Redpanda
3. Point Flink TelemetryIntakeJob → Redpanda
4. Point SDKs → Redpanda (change bootstrap server in Telemetry API config)

#### 5d: Enable tiered storage

```yaml
# Redpanda tiered storage — cold data offloaded to S3 automatically
tieredStorage:
  enabled: true
  bucket: your-bucket
  region: ap-south-1
  credentialsSecretName: s3-credentials
```

After this: Kafka topics `telemetry.raw` and `telemetry.denorm` can be deleted (they were already shadow-only after Phase 0 optimizations).

**Output:** Redpanda serving all traffic. Old Kafka cluster decommissioned.

---

### Phase 6: Real-Time Insights (Weeks 35–40)

**Goal:** Live dashboards with <5 second data freshness. Real-time alerting.

**Tasks:**

#### 6a: ClickHouse materialized view library

Build the following pre-aggregated views (all update in real-time as events arrive):

| View | Description | Refresh |
|---|---|---|
| `sessions_by_channel_hourly` | Sessions per channel per hour | Real-time |
| `content_engagement_daily` | Plays, completions, time-spent per content | Real-time |
| `user_activity_daily` | Active users by state, district | Real-time |
| `error_rate_by_app` | Error events per app per 5-min window | Real-time |
| `workflow_completion_funnel` | START→INTERACT→END funnel | Real-time |
| `device_distribution` | Events by device type, OS, SDK version | Real-time |

#### 6b: Superset dashboard library

| Dashboard | Audience | Data Source |
|---|---|---|
| Platform Health | Engineering / Ops | ClickHouse + Grafana |
| Content Performance | Product / L&D | ClickHouse Superset |
| User Engagement | Karmayogi Admins | ClickHouse Superset |
| State-wise Analytics | Policy / Government | ClickHouse Superset |
| Data Quality | Data Engineering | ClickHouse + Grafana |

#### 6c: Grafana alerting on Flink + Redpanda

```yaml
# Grafana alert: consumer lag > 10k for > 2 minutes
- name: HighConsumerLag
  condition: kafka_consumergroup_lag_sum > 10000
  for: 2m
  annotations:
    summary: "Flink {{ $labels.job }} falling behind"
    description: "Lag {{ $value }} on topic {{ $labels.topic }}"
```

#### 6d: Real-time anomaly detection (optional)

Flink job that computes a 5-minute rolling baseline and alerts if:
- Event rate drops by >30% (SDK issue)
- Error rate increases by >5x (deployment issue)
- Dedup rate increases by >50% (replay or SDK bug)

---

### Phase 7: Decommission Legacy (Weeks 41–48)

**Goal:** Remove Druid, SECOR, old Redis, old Kafka. Clean infrastructure.

**Tasks:**

| Item | Prerequisite | Action |
|---|---|---|
| SECOR | Iceberg has 60 days of data, Trino queries validated | `kubectl scale deployment secor --replicas=0`, delete consumer groups |
| Apache Druid | ClickHouse parity validated for 30+ days, all dashboards migrated | Scale down MiddleManager, Historical, Coordinator, Broker, Overlord, Router |
| Old Redis | DragonflyDB running stably 30+ days | Delete Redis StatefulSet |
| Old Kafka | Redpanda running stably 30+ days | Delete Kafka StatefulSet, delete ZooKeeper StatefulSet |
| Shadow topics (`telemetry.raw`, `telemetry.denorm`) | Already from Phase 0 optimization | Delete from Redpanda |

**Post-decommission infrastructure:**

```
Before (current):               After (new):
├── Kafka (3–5 nodes)           ├── Redpanda (3 nodes) — handles Kafka + SECOR role
├── ZooKeeper (3 nodes)         ├── ClickHouse (3 nodes)
├── Flink (3 jobs)              ├── Flink (2 jobs, Java 17, 2.0)
├── Druid (6 services)          ├── DragonflyDB (1–2 nodes)
├── Redis (1–3 nodes)           ├── Apache Superset
├── SECOR                       ├── Trino (for historical Iceberg)
└── Grafana                     └── Grafana + OpenTelemetry
```

**Node count reduction: ~18–22 nodes → 10–12 nodes**

---

## 8. Infrastructure Cost Comparison

### Current Estimated Monthly Cost (Production)

| Component | Instance Type | Count | Monthly Cost (USD) |
|---|---|---|---|
| Kafka brokers | m5.2xlarge | 5 | $1,400 |
| ZooKeeper | m5.xlarge | 3 | $420 |
| Flink JobManager | m5.xlarge | 3 | $420 |
| Flink TaskManager | m5.4xlarge | 6–12 | $3,360 |
| Druid Historical | r5.4xlarge | 4 | $3,360 |
| Druid MiddleManager | m5.2xlarge | 2 | $560 |
| Druid Coordinator/Broker | m5.xlarge | 4 | $560 |
| Redis | r5.xlarge | 2 | $480 |
| SECOR | m5.large | 2 | $190 |
| **Total** | | | **~$10,750/month** |

### New Architecture Estimated Monthly Cost

| Component | Instance Type | Count | Monthly Cost (USD) |
|---|---|---|---|
| Redpanda brokers | r6i.4xlarge | 3 | $2,520 |
| Flink TaskManager | m6i.4xlarge | 4–8 | $2,240 |
| Flink JobManager | m6i.xlarge | 2 | $280 |
| ClickHouse cluster | r6i.8xlarge | 3 | $5,040 |
| DragonflyDB | r6i.2xlarge | 2 | $840 |
| Trino | m6i.2xlarge | 2 | $560 |
| Superset | m6i.large | 1 | $140 |
| S3/GCS (Iceberg) | Storage | — | $200 (compressed) |
| **Total** | | | **~$11,820/month** |

**Note:** Raw cost is similar, but the new architecture provides:
- 10x higher throughput capacity (not yet utilized — massive headroom)
- Real-time insights (currently impossible)
- Full historical SQL access (currently impossible without loading into Druid)
- 60% lower cost once scaled to 1M+ TPS (Redpanda and ClickHouse scale more efficiently than Kafka + Druid)

At 500k TPS (5x current), estimated cost delta:
- Current approach (scaled): ~$35,000/month (linear Kafka + Druid scaling)
- New architecture (scaled): ~$18,000/month (Redpanda tiered storage + ClickHouse columnar efficiency)

---

## 9. Real-Time Insights Layer

This is the biggest *new capability* that the current architecture simply cannot provide.

### What "Real-Time" Means Here

| Metric | Current | New |
|---|---|---|
| Data freshness in dashboards | 30–120 seconds (Druid ingestion lag) | 1–5 seconds (ClickHouse Kafka engine) |
| Dashboard query latency | 1–5 seconds | 10–100 ms |
| Alerting on anomalies | Manual, next-day | Automated, within 30 seconds |
| Historical data access | Druid only (limited SQL) | Trino on Iceberg (full SQL, years of data) |

### Sample Real-Time Insights

#### 1. Live Platform Health (Engineering)
```
┌──────────────────────────────────────────────────────────┐
│  PLATFORM HEALTH — Last 5 Minutes                        │
│  Updated: 2 seconds ago                                  │
├────────────────┬─────────────────┬────────────────────── ┤
│  Events/sec    │  Error Rate     │  Active Users          │
│  64,231        │  0.02%          │  12,847               │
│  ▲ 12% vs avg  │  ✓ Normal       │  ▼ 3% vs last hour    │
├────────────────┴─────────────────┴──────────────────────┤
│  Consumer Lag (Flink): 234 events — 0.004 seconds        │
│  Dedup Rate: 1.2% (normal: <5%)                         │
└──────────────────────────────────────────────────────────┘
```

#### 2. Content Engagement Real-Time (Product)
```sql
-- This query runs in <50ms on ClickHouse materialized view
SELECT
    content_id,
    uniqExact(actor_id) AS live_users,
    count(*) AS events_last_5min
FROM telemetry
WHERE toDateTime(ets/1000) >= now() - INTERVAL 5 MINUTE
  AND eid = 'INTERACT'
GROUP BY content_id
ORDER BY live_users DESC
LIMIT 10;
```

#### 3. Geographic Real-Time Map
Every AUDIT event (user login) updates `user_activity_daily` materialized view. Superset map shows state-wise logins updated every 30 seconds, with no Druid query required.

#### 4. Anomaly Detection Alert
```
Flink rolling window (5-minute tumbling):
  IF events_per_second < (avg_last_7days * 0.7) FOR 3 consecutive minutes:
    → Alert: Grafana PagerDuty / Slack
    → "Event rate 35% below baseline — SDK or API issue likely"
```

---

## 10. Risk & Migration Strategy

### Risk Matrix

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ClickHouse performance doesn't match Druid for specific query patterns | Medium | High | Run 8-week parallel validation before cutting over dashboards |
| Redpanda compatibility issue with existing SDK Kafka client version | Low | High | Test in staging with 5% traffic mirror before production |
| Iceberg write adds latency to Flink EnrichmentJob | Medium | Medium | Async Iceberg sink (non-blocking); monitor job latency |
| Flink 2.0 API breaking changes in custom operators | Medium | Medium | Migrate one job at a time; regression test each job in staging |
| Schema enforcement breaks existing SDK events | High | High | Run schema validation in "log only" mode for 4 weeks before enforcing reject |
| DragonflyDB compatibility issue with Lettuce client | Low | Low | Lettuce is Valkey/DragonflyDB tested; fallback to Redis if issue |

### Migration Principles

1. **Parallel run everything** — every new component runs alongside the old one for 30+ days before cutover
2. **No big bangs** — each phase is independently deployable and rollback-able
3. **Validate with data, not just health checks** — compare event counts, user counts, and query results between old and new
4. **Keep decommission last** — never remove a component until its replacement has run stably in production for 30+ days

### Rollback Capability per Phase

| Phase | Rollback Mechanism | Time to Rollback |
|---|---|---|
| Phase 2 (ClickHouse) | Stop ClickHouse Kafka consumer — Druid still running | Minutes |
| Phase 3 (Iceberg) | Remove Iceberg sink from Flink job — revert to SECOR | 1 deploy cycle |
| Phase 4 (Flink 2.0) | Switch k8s deployment to old image — state in RocksDB is compatible | Minutes |
| Phase 5 (Redpanda) | Switch bootstrap server back to Kafka in Flink config — SDK doesn't change | Minutes |
| Phase 7 (Decommission) | Cannot rollback Druid after deletion — this is why 30-day validation is mandatory | N/A |

---

## 11. Success Metrics

### Throughput Targets

| Milestone | Metric | Target | Measurement |
|---|---|---|---|
| Phase 2 complete | ClickHouse query latency | p50 <100ms, p99 <500ms | Superset slow query log |
| Phase 4 complete | Flink throughput | 500k events/sec sustained | Flink metrics dashboard |
| Phase 5 complete | Redpanda producer p99 | <5ms | Redpanda console metrics |
| Phase 6 complete | Dashboard freshness | <5 seconds | Superset: `now() - max(indexed_at)` |
| Phase 7 complete | Infrastructure cost | <$15k/month at 300k TPS | Cloud billing |

### Data Quality Targets

| Metric | Target |
|---|---|
| ClickHouse vs Druid daily event count | <0.01% discrepancy for 30 consecutive days |
| Iceberg vs SECOR row count | <0.001% discrepancy for 30 consecutive days |
| Schema validation rejection rate | <0.1% of events (once enforced) |
| Dedup false positive rate | 0% (verified by mid uniqueness check in ClickHouse) |

### Operational Targets

| Metric | Current | Target |
|---|---|---|
| Time to investigate data issue | Hours (dig through logs + SECOR files) | Minutes (SQL on Iceberg or ClickHouse) |
| Time to add new dashboard metric | Days (Druid datasource reconfiguration) | Hours (new ClickHouse materialized view) |
| Kubernetes node count | 18–22 | 10–12 |
| Jobs in hot path | 2 | 2 (same, but faster per job) |
| Kafka topics in hot path | 6 (after P0 optimization) | 4 (further reduced) |

---

## Appendix: Technology References

| Tool | Docs | Version |
|---|---|---|
| Apache Flink | https://flink.apache.org | 2.0 |
| Redpanda | https://redpanda.com/docs | 24.x |
| Apache ClickHouse | https://clickhouse.com/docs | 24.x |
| Apache Iceberg | https://iceberg.apache.org/docs | 1.5 |
| DragonflyDB | https://dragonflydb.io/docs | 1.x |
| Apache Superset | https://superset.apache.org/docs | 4.x |
| Trino | https://trino.io/docs/current | 450 |
| Apache Avro | https://avro.apache.org/docs | 1.11 |
| OpenTelemetry | https://opentelemetry.io/docs | 1.x |
| Flink-Iceberg connector | https://iceberg.apache.org/docs/latest/flink | 1.5 |

---

*This document should be reviewed and updated quarterly as the implementation progresses. Each phase's completion should be recorded in a PHASE_COMPLETION_LOG.md alongside this file.*
