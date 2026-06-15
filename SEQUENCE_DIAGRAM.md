# Sunbird Data Pipeline - Detailed Sequence Diagram

## Kafka Cluster Layout

All topics listed below live in the **same Kafka cluster** (single-cluster deployment).
Topic naming follows `{env}.{topic}` — e.g., `staging.telemetry.ingest`.

```
KAFKA CLUSTER
├─ Ingestion boundary (SDK → pipeline)
│   ├─ {env}.telemetry.ingestion        ← SDK / Telemetry API writes here (external contract)
│   └─ {env}.telemetry.raw.event        ← Pre-formatted individual events (non-batch ingestion path)
│
├─ Pipeline hot path
│   ├─ {env}.telemetry.ingest           ← ingest-router output; telemetry-extractor input
│   ├─ {env}.telemetry.raw              ← extractor output + ingest-router raw output; preprocessor input
│   ├─ {env}.telemetry.unique           ← preprocessor primary output; de-norm input
│   ├─ {env}.telemetry.unique.secondary ← preprocessor secondary output; de-norm input
│   ├─ {env}.telemetry.denorm           ← de-norm output; druid-validator input
│   ├─ {env}.druid.events.telemetry     ← final Druid telemetry feed
│   └─ {env}.druid.events.summary       ← final Druid summary feed
│
├─ Side / error channels
│   ├─ {env}.telemetry.duplicate        ← duplicate events (all jobs)
│   ├─ {env}.telemetry.failed           ← validation/size failures (all jobs)
│   ├─ {env}.telemetry.audit            ← AUDIT events
│   ├─ {env}.telemetry.cb.audit         ← CB_AUDIT events
│   ├─ {env}.telemetry.log              ← LOG events
│   ├─ {env}.telemetry.error            ← ERROR events
│   ├─ {env}.telemetry.assess.raw       ← ASSESS/RESPONSE events (post-redaction)
│   └─ {env}.druid.events.log           ← LOG events routed to Druid
│
└─ Archival (SECOR consumers)
    ├─ {env}.telemetry.raw              ← SECOR backup group
    └─ {env}.telemetry.denorm           ← 3 SECOR backup groups (denorm, channel, summary)
```

> **Ingest-router significance:** There are TWO topics at the ingestion boundary — `telemetry.ingestion`
> (where SDKs write) and `telemetry.ingest` (where the pipeline reads). The `ingest-router` Flink job
> is the only bridge between them. If ingest-router is down, the entire pipeline receives zero events.
> Its consumer group `{env}-ingest-router-group` is the primary lag alert for pipeline health.

---

## Complete Data Flow Sequence

```
TIME →

┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                                          │
│  TELEMETRY API                                                                                          │
│  (Mobile/Web Frontend)                                                                                  │
│         │                                                                                                │
│         │ Captures user click events (CLICK, VIEW, INTERACT, ASSESS, etc.)                            │
│         │ Groups into batch: { mid, params: {msgid}, channel, did, events: [...] }                     │
│         │                                                                                                │
│         ▼                                                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐                           │
│  │ KAFKA TOPIC: {env}.telemetry.ingestion                                  │                           │
│  │ External entry point — SDK / Telemetry API writes here                  │                           │
│  │ Format: Raw bytes (batch JSON, not deserialized by broker)              │                           │
│  └───────────────────────┬─────────────────────────────────────────────────┘                           │
│                           │                                                                              │
│                           │ kafkaBytesSource (parallelism = task.consumer.parallelism)                 │
│                           │                                                                              │
│                           ▼                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ FLINK JOB: ingest-router  [GATEWAY — runs 2 replicas, 2 task slots]                        │        │
│  │                                                                                              │        │
│  │  ┌─ INGEST STREAM ──────────────────────────────────────────────────────────────────────┐  │        │
│  │  │  Source: {env}.telemetry.ingestion   Consumer: ingest-router-consumer                │  │        │
│  │  │  Sink:   {env}.telemetry.ingest      Producer: ingest-router-sink                    │  │        │
│  │  │  Logic:  kafkaBytesSource → kafkaBytesSink  (NO deserialization, NO transformation) │  │        │
│  │  │  Metric: ingest-success-event-count                                                   │  │        │
│  │  └──────────────────────────────────────────────────────────────────────────────────────┘  │        │
│  │                                                                                              │        │
│  │  ┌─ RAW STREAM (parallel, independent) ─────────────────────────────────────────────────┐  │        │
│  │  │  Source: {env}.telemetry.raw.event   Consumer: raw-router-consumer                   │  │        │
│  │  │  Sink:   {env}.telemetry.raw         Producer: raw-router-sink                       │  │        │
│  │  │  Logic:  kafkaBytesSource → kafkaBytesSink  (NO deserialization, NO transformation) │  │        │
│  │  │  Purpose: ingestion path for pre-formatted individual events (non-batch)             │  │        │
│  │  │  Metric: raw-success-event-count                                                      │  │        │
│  │  └──────────────────────────────────────────────────────────────────────────────────────┘  │        │
│  │                                                                                              │        │
│  │  WHY THIS JOB EXISTS:                                                                       │        │
│  │  1. Topic contract isolation: SDKs write to .ingestion; pipeline reads from .ingest         │        │
│  │     Either side can be reconfigured without touching the other.                             │        │
│  │  2. Checkpoint boundary: Flink manages offset commits here — if pipeline lags,             │        │
│  │     consumer group {env}-ingest-router-group is the first alert signal.                    │        │
│  │  3. Monitoring seam: named metrics + Helm alertrules on this group specifically.            │        │
│  │  4. Parallelism isolation per stream: ingest and raw paths tuned independently.             │        │
│  │                                                                                              │        │
│  │  CRITICAL: If ingest-router is down, telemetry-extractor receives ZERO events.             │        │
│  │  The entire pipeline goes dark immediately.                                                  │        │
│  └────────────────────────────────────────────────────────────────────────────────────────────┘        │
│         │                                     │                                                          │
│         │ (ingest stream output)              │ (raw stream output — merges at preprocessor)            │
│         ▼                                     ▼                                                          │
│  ┌──────────────────────────────┐   ┌─────────────────────────────┐                                    │
│  │ KAFKA: telemetry.ingest      │   │ KAFKA: telemetry.raw        │◄─── also written by extractor      │
│  │ Internal pipeline entry      │   │ Pre-formatted events         │                                    │
│  │ (1 batch = 100-1000 events)  │   │ (individual, not batch)      │                                    │
│  └──────────────┬───────────────┘   └──────────────┬──────────────┘                                    │
│                  │                                   │                                                   │
│                  │ Consume in parallel (parallelism=4)│                                                  │
│                  │                                   │ (feeds pipeline-preprocessor directly)            │
│                  ▼                                   │                                                   │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ FLINK JOB: Telemetry Extractor                                                              │        │
│  │ ├─ DeduplicationFunction (Check batch msgid in Redis)                                       │        │
│  │ │  ├─ IF msgid exists in Redis → DUPLICATE BATCH                                           │        │
│  │ │  │                            → Kafka: telemetry.duplicate                               │        │
│  │ │  │                            → Mark flag: extractor_duplicate=true                       │        │
│  │ │  │                                                                                        │        │
│  │ │  └─ IF msgid NOT in Redis → UNIQUE BATCH                                                 │        │
│  │ │                           → Add msgid to Redis (TTL: 24h)                               │        │
│  │ │                           → Continue to ExtractionFunction                               │        │
│  │ │                                                                                            │        │
│  │ ├─ ExtractionFunction (For each event in batch):                                            │        │
│  │ │  ├─ Extract event from events[]                                                          │        │
│  │ │  ├─ Update @timestamp and syncts                                                         │        │
│  │ │  ├─ Check event size < 1 MB                                                              │        │
│  │ │  │  ├─ IF size > 1 MB → Kafka: telemetry.failed                                         │        │
│  │ │  │  │                  → Mark flag: ex_processed=false                                    │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ IF size OK → Check event type (eid):                                              │        │
│  │ │  │                 ├─ IF ASSESS or RESPONSE → assess-redact-events tag                   │        │
│  │ │  │                 ├─ IF LOG → log-events tag                                            │        │
│  │ │  │                 └─ ELSE → raw-events tag (TO telemetry.raw)                           │        │
│  │ │  │                           Mark flag: ex_processed=true                                │        │
│  │ │  │                                                                                        │        │
│  │ │  └─ Generate AUDIT event (batch summary)                                                │        │
│  │ │                                                                                            │        │
│  │ └─ (OPTIONAL) RedactorFunction (For ASSESS/RESPONSE):                                       │        │
│  │    ├─ Remove sensitive fields based on questionType                                         │        │
│  │    └─ Send to telemetry.assess.raw                                                          │        │
│  │                                                                                              │        │
│  │ METRICS INCREMENTED:                                                                        │        │
│  │ • success-batch-count (unique batches)                                                      │        │
│  │ • success-event-count (events extracted)                                                    │        │
│  │ • audit-event-count (audit events generated)                                                │        │
│  └────────────────────────────────────────────────────────────────────────────────────────────┘        │
│         │                                                                                                │
│         ├─► Kafka: telemetry.raw (main output)                                                │        │
│         ├─► Kafka: telemetry.duplicate (if duplicate batch)                                    │        │
│         ├─► Kafka: telemetry.failed (if size > 1MB)                                            │        │
│         ├─► Kafka: telemetry.audit (audit events)                                              │        │
│         └─► Kafka: telemetry.assess.raw (assessment events)                                    │        │
│         │                                                                                        │        │
│         │ Consume in parallel (parallelism=4)                                                   │        │
│         │                                                                                        │        │
│         ▼                                                                                        │        │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ FLINK JOB: Pipeline Preprocessor                                                            │        │
│  │ ├─ TelemetryValidationFunction:                                                             │        │
│  │ │  ├─ Data Correction:                                                                      │        │
│  │ │  │  ├─ Remove federated user ID prefix ("f:" prefix)                                      │        │
│  │ │  │  ├─ Set default channel = "org.sunbird" if missing                                     │        │
│  │ │  │  ├─ Correct dialCode key (dialCodes → dialcode)                                        │        │
│  │ │  │  └─ Correct dialCode value if DialCode/QR object                                       │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ Schema Validation:                                                                    │        │
│  │ │  │  ├─ Load schema for event type (eid)                                                   │        │
│  │ │  │  ├─ IF schema NOT found → Kafka: telemetry.failed                                      │        │
│  │ │  │  │                       → Mark: pp_validation_processed=true (error)                   │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ IF schema found → Validate event structure                                         │        │
│  │ │  │                      ├─ IF valid → Continue                                            │        │
│  │ │  │                      │              Mark: pp_validation_processed=true                 │        │
│  │ │  │                      │                                                                │        │
│  │ │  │                      └─ IF invalid → Kafka: telemetry.failed                          │        │
│  │ │  │                                     → Mark: pp_validation_processed=true (error)       │        │
│  │ │  │                                                                                        │        │
│  │ │  └─ Deduplication Decision:                                                               │        │
│  │ │     ├─ IF producerId in dedup_included_ids (e.g., "prod.diksha.portal")                   │        │
│  │ │     │  ├─ Check mid in Redis                                                             │        │
│  │ │     │  ├─ IF mid exists → Kafka: telemetry.duplicate                                     │        │
│  │ │     │  │                  Mark: pp_duplicate=true                                        │        │
│  │ │     │  │                                                                                 │        │
│  │ │     │  └─ IF mid NOT exists → Add to Redis (TTL: 24h)                                    │        │
│  │ │     │                        → Continue to Router                                         │        │
│  │ │     │                        → Mark: pp_duplicate=false                                   │        │
│  │ │     │                                                                                     │        │
│  │ │     └─ IF producerId NOT in dedup_included_ids → Skip dedup                              │        │
│  │ │        → Mark: pp_duplicate_skipped=true                                                  │        │
│  │ │        → Continue to Router                                                               │        │
│  │ │                                                                                            │        │
│  │ ├─ TelemetryRouterFunction:                                                                 │        │
│  │ │  ├─ Route by event type:                                                                  │        │
│  │ │  │                                                                                        │        │
│  │ │  │  IF isLogEvent:                                                                       │        │
│  │ │  │  → Kafka: telemetry.log (NO denormalization)                                          │        │
│  │ │  │  → Return (stop processing)                                                           │        │
│  │ │  │                                                                                        │        │
│  │ │  │  IF isErrorEvent:                                                                     │        │
│  │ │  │  → Kafka: telemetry.error (NO denormalization)                                        │        │
│  │ │  │  → Return (stop processing)                                                           │        │
│  │ │  │                                                                                        │        │
│  │ │  │  IF isAuditEvent:                                                                     │        │
│  │ │  │  → Kafka: telemetry.audit                                                             │        │
│  │ │  │  → ALSO continue to denorm routing (dual output)                                       │        │
│  │ │  │                                                                                        │        │
│  │ │  │  IF isShareEvent:                                                                     │        │
│  │ │  │  → Send to ShareEventsFlattenerFunction                                               │        │
│  │ │  │                                                                                        │        │
│  │ │  │  ELSE (Regular telemetry):                                                            │        │
│  │ │  │  → Check if priority event (CLICK, VIEW, INTERACT, etc.)                              │        │
│  │ │  │  ├─ IF priority → Kafka: telemetry.denorm.primary (faster processing)                 │        │
│  │ │  │  └─ IF low priority → Kafka: telemetry.denorm.secondary (batch processing)            │        │
│  │ │  │                                                                                        │        │
│  │ │  └─ (OPTIONAL) ShareEventsFlattenerFunction:                                              │        │
│  │ │     ├─ For each item in share event:                                                     │        │
│  │ │     │  ├─ IF transfers = 0 → type = "download"                                           │        │
│  │ │     │  ├─ IF transfers > 0 → type = "import"                                             │        │
│  │ │     │  └─ Create individual SHARE_ITEM event                                             │        │
│  │ │     │                                                                                     │        │
│  │ │     └─ Move object data to rollup.l1 for easier querying                                  │        │
│  │ │                                                                                            │        │
│  │ │ METRICS INCREMENTED:                                                                      │        │
│  │ │ • validation-success-event-count                                                          │        │
│  │ │ • validation-failed-event-count                                                           │        │
│  │ │ • duplicate-event-count (if duplicate)                                                    │        │
│  │ │ • duplicate-skipped-event-count (if dedup skipped)                                        │        │
│  │ │ • denorm-primary-route-success-count / denorm-secondary-route-success-count               │        │
│  │ │                                                                                            │        │
│  │ └─► (INTERMEDIATE) Denorm Stream (to next job)                                              │        │
│  └────────────────────────────────────────────────────────────────────────────────────────────┘        │
│         │                                                                                                │
│         ├─► Kafka: telemetry.denorm.primary (HIGH priority)                                   │        │
│         ├─► Kafka: telemetry.denorm.secondary (LOW priority)                                  │        │
│         ├─► Kafka: telemetry.failed (validation failed)                                       │        │
│         ├─► Kafka: telemetry.duplicate (event duplicate)                                      │        │
│         ├─► Kafka: telemetry.log (log events - bypassed)                                      │        │
│         ├─► Kafka: telemetry.error (error events - bypassed)                                  │        │
│         ├─► Kafka: telemetry.audit (audit events - also to denorm)                            │        │
│         └─► Kafka: telemetry.cb.audit (cross-browser audit)                                   │        │
│         │                                                                                        │        │
│         │ Consume in parallel: primary (parallelism=4), secondary (parallelism=2)              │        │
│         │                                                                                        │        │
│         ▼                                                                                        │        │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ FLINK JOB: De-normalization (Enrichment)                                                    │        │
│  │ ├─ DenormalizationFunction:                                                                 │        │
│  │ │                                                                                            │        │
│  │ │  ├─ Event Age Check:                                                                      │        │
│  │ │  │  ├─ IF event.timestamp < 3 months ago (config: telemetry.ignore.period.months)       │        │
│  │ │  │  │  └─ → Kafka: telemetry.failed                                                     │        │
│  │ │  │  │     Increment: events-expired counter                                              │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ Continue if recent                                                                │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ Event Type Filter:                                                                    │        │
│  │ │  │  ├─ IF event.eid contains "SUMMARY" (except ME_WORKFLOW_SUMMARY)                      │        │
│  │ │  │  │  ├─ OR event.eid in skip.events (e.g., LOG, ERROR)                                 │        │
│  │ │  │  │  └─ → Skip denorm, send to next kafka topic                                        │        │
│  │ │  │  │     Increment: events-skipped counter                                              │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ Continue for regular/ME_WORKFLOW_SUMMARY events                                    │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ Fetch All Denorm Data (Single Redis call to get all caches):                         │        │
│  │ │  │  ├─ Collect required IDs: did, actor.id, object.id, etc.                              │        │
│  │ │  │  └─ Call denormCache.getDenormData(event)                                             │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ DeviceDenormalization:                                                                │        │
│  │ │  │  ├─ IF event.did (device ID) exists:                                                  │        │
│  │ │  │  │  ├─ Query Redis: redis-meta.device.host:6379 (DB 0)                                │        │
│  │ │  │  │  ├─ Fetch fields: country, state, state_code, city, district, devicespec...       │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  ├─ IF found (cache HIT):                                                          │        │
│  │ │  │  │  │  ├─ Derive ISO state code (if state present)                                    │        │
│  │ │  │  │  │  ├─ Add to event.devicedata = {...}                                            │        │
│  │ │  │  │  │  └─ Increment: device-cache-hit                                                │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  └─ IF NOT found (cache MISS):                                                     │        │
│  │ │  │  │     └─ Increment: device-cache-miss                                                │        │
│  │ │  │  │        (Continue without device data)                                              │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ Increment: device-total                                                           │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ UserDenormalization:                                                                  │        │
│  │ │  │  ├─ IF event.actor.id (user ID) exists AND actor.type = "User":                       │        │
│  │ │  │  │  ├─ Query Redis: redis-meta.user.host:6379 (DB 0)                                  │        │
│  │ │  │  │  ├─ Fetch fields: usertype, grade, language, state, district, subject...          │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  ├─ IF found (cache HIT):                                                          │        │
│  │ │  │  │  │  ├─ Add to event.userdata = {...}                                              │        │
│  │ │  │  │  │  └─ Increment: user-cache-hit                                                  │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  └─ IF NOT found (cache MISS):                                                     │        │
│  │ │  │  │     └─ Increment: user-cache-miss                                                 │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ Increment: user-total                                                             │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ DialcodeDenormalization:                                                              │        │
│  │ │  │  ├─ IF event.object.type = "dialcode" or "qr":                                        │        │
│  │ │  │  │  ├─ Query Redis: redis-meta.dialcode.host:6379 (DB 0)                              │        │
│  │ │  │  │  ├─ Fetch fields: channel, publisher, generated_on, published_on, status...       │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  ├─ Convert timestamps to epoch (if date format)                                    │        │
│  │ │  │  │  ├─ Add to event.dialcodedata = {...}                                              │        │
│  │ │  │  │  └─ Track: dialcode-total, dialcode-cache-hit, dialcode-cache-miss                │        │
│  │ │  │  │                                                                                     │        │
│  │ │  ├─ ContentDenormalization:                                                               │        │
│  │ │  │  ├─ IF event.object.type NOT (dialcode, qr, user):                                    │        │
│  │ │  │  │  ├─ Query Redis: redis-meta.content.host:6379 (DB 0)                               │        │
│  │ │  │  │  ├─ Fetch fields: name, contentType, framework, subject, board, channel...        │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  ├─ Add to event.contentdata = {...}                                               │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  ├─ IF event.rollup.l1 exists AND rollup.l1 != object.id:                          │        │
│  │ │  │  │  │  ├─ Fetch collection (parent) data from Redis                                   │        │
│  │ │  │  │  │  └─ Add to event.collectiondata = {...}                                         │        │
│  │ │  │  │  │     Track: coll-total, coll-cache-hit, coll-cache-miss                         │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  │  ├─ Convert timestamps to epoch (if date format)                                    │        │
│  │ │  │  │  └─ Track: content-total, content-cache-hit, content-cache-miss                   │        │
│  │ │  │  │                                                                                     │        │
│  │ │  ├─ LocationDenormalization (Derived Location with Fallback):                            │        │
│  │ │  │  ├─ Priority 1: User profile location (from userdata)                                 │        │
│  │ │  │  │  ├─ IF userdata.state exists → derivedLocation = userdata.state                    │        │
│  │ │  │  │  │                            → derivedFrom = "user_profile"                        │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  ├─ Priority 2: User declared location (from devicedata)                              │        │
│  │ │  │  │  ├─ IF derivedLocation empty AND devicedata.user_declared_state exists             │        │
│  │ │  │  │  │  → derivedLocation = devicedata.user_declared_state                             │        │
│  │ │  │  │  │  → derivedFrom = "user_declared"                                                │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  ├─ Priority 3: IP-based location (from devicedata)                                   │        │
│  │ │  │  │  ├─ IF derivedLocation empty AND devicedata.state exists (IP-based)               │        │
│  │ │  │  │  │  → derivedLocation = devicedata.state                                           │        │
│  │ │  │  │  │  → derivedFrom = "ip_location"                                                 │        │
│  │ │  │  │  │                                                                                  │        │
│  │ │  │  └─ Add to event.derivedlocationdata = {location, derivedFrom}                        │        │
│  │ │  │     Track: loc-total, loc-cache-hit, loc-cache-miss                                   │        │
│  │ │  │                                                                                        │        │
│  │ │  └─ Add denorm flags to event (track which enrichments applied)                           │        │
│  │ │                                                                                            │        │
│  │ │ METRICS INCREMENTED:                                                                      │        │
│  │ │ • device-total, device-cache-hit, device-cache-miss                                       │        │
│  │ │ • user-total, user-cache-hit, user-cache-miss                                             │        │
│  │ │ • content-total, content-cache-hit, content-cache-miss                                    │        │
│  │ │ • dialcode-total, dialcode-cache-hit, dialcode-cache-miss                                 │        │
│  │ │ • loc-total, loc-cache-hit, loc-cache-miss                                                │        │
│  │ │ • events-skipped, events-expired                                                          │        │
│  │ │                                                                                            │        │
│  │ └─ Output enriched events to Kafka (denorm events tag)                                       │        │
│  └────────────────────────────────────────────────────────────────────────────────────────────┘        │
│         │                                                                                                │
│         ├─► Kafka: telemetry.unique.primary (from denorm.primary input)                       │        │
│         ├─► Kafka: telemetry.unique.secondary (from denorm.secondary input)                   │        │
│         ├─► Kafka: telemetry.failed (expired events, errors)                                  │        │
│         │                                                                                        │        │
│         │ Consume in parallel (parallelism=4)                                                   │        │
│         │ Note: Both primary and secondary merge at this stage                                 │        │
│         │                                                                                        │        │
│         ▼                                                                                        │        │
│  ┌────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ FLINK JOB: Druid Events Validator                                                           │        │
│  │ ├─ DruidValidatorFunction:                                                                  │        │
│  │ │                                                                                            │        │
│  │ │  ├─ Optional: Deduplication (if task.druid.deduplication.enabled = true)                  │        │
│  │ │  │  ├─ Check event.mid in Redis (Dedup store)                                             │        │
│  │ │  │  ├─ IF mid exists:                                                                     │        │
│  │ │  │  │  ├─ → Kafka: telemetry.duplicate                                                   │        │
│  │ │  │  │  ├─ Mark flag: dv_duplicate = true                                                 │        │
│  │ │  │  │  └─ Return (stop processing)                                                       │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ IF mid NOT exists:                                                                │        │
│  │ │  │     ├─ Add to Redis (TTL: 24h)                                                         │        │
│  │ │  │     └─ Continue                                                                        │        │
│  │ │  │                                                                                        │        │
│  │ │  ├─ Optional: Schema Validation (if task.druid.validation.enabled = true)                 │        │
│  │ │  │  ├─ Load Druid-specific schema (search.json or telemetry.json)                         │        │
│  │ │  │  ├─ Validate denormalized event structure                                              │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  ├─ IF validation SUCCESS:                                                            │        │
│  │ │  │  │  ├─ Mark event.validated = true                                                     │        │
│  │ │  │  │  ├─ Increment: validation-success-message-count                                     │        │
│  │ │  │  │  └─ Continue to routing                                                            │        │
│  │ │  │  │                                                                                     │        │
│  │ │  │  └─ IF validation FAILURE:                                                            │        │
│  │ │  │     ├─ Mark event as invalid                                                           │        │
│  │ │  │     ├─ → Kafka: telemetry.failed                                                      │        │
│  │ │  │     ├─ Increment: validation-failed-message-count                                      │        │
│  │ │  │     └─ Return (stop processing)                                                       │        │
│  │ │  │                                                                                        │        │
│  │ │  └─ Event Routing (Final Step):                                                           │        │
│  │ │     ├─ IF event.isSummaryEvent (ME_WORKFLOW_SUMMARY, ME_SESSION_SUMMARY):                 │        │
│  │ │     │  ├─ → output tag: summary-router                                                    │        │
│  │ │     │  ├─ → Kafka: druid.events.summary                                                  │        │
│  │ │     │  └─ Increment: summary-route-success-count                                          │        │
│  │ │     │                                                                                     │        │
│  │ │     └─ ELSE (Regular telemetry event):                                                    │        │
│  │ │        ├─ → output tag: telemetry-router                                                  │        │
│  │ │        ├─ → Kafka: druid.events.telemetry                                                │        │
│  │ │        └─ Increment: telemetry-route-success-count                                        │        │
│  │ │                                                                                            │        │
│  │ │ METRICS INCREMENTED:                                                                      │        │
│  │ │ • validation-success-message-count                                                        │        │
│  │ │ • validation-failed-message-count                                                         │        │
│  │ │ • telemetry-route-success-count                                                           │        │
│  │ │ • summary-route-success-count                                                             │        │
│  │ │                                                                                            │        │
│  │ └─ Final validated events ready for Druid ingestion                                          │        │
│  └────────────────────────────────────────────────────────────────────────────────────────────┘        │
│         │                                                                                                │
│         ├─► Kafka: druid.events.telemetry (FINAL OUTPUT - TELEMETRY)                          │        │
│         ├─► Kafka: druid.events.summary (FINAL OUTPUT - SUMMARY)                              │        │
│         ├─► Kafka: telemetry.failed (validation failed)                                       │        │
│         ├─► Kafka: telemetry.duplicate (summary duplicates)                                   │        │
│         │                                                                                        │        │
│         │ External Process (Druid Indexing):                                                   │        │
│         │ ├─ Kafka consumers read from druid.events.telemetry                                  │        │
│         │ ├─ Parse events                                                                       │        │
│         │ └─ Ingest into Druid datasources                                                     │        │
│         │                                                                                        │        │
│         ▼                                                                                        │        │
│  ┌──────────────────────────────┐                                                              │        │
│  │ DRUID DATASOURCES            │                                                              │        │
│  │ ├─ events (TELEMETRY)        │ ← Queried by analytics dashboards                            │        │
│  │ ├─ events_summary (SUMMARY)  │                                                              │        │
│  │ └─ events_raw (backup)       │                                                              │        │
│  └──────────────────────────────┘                                                              │        │
│         │                                                                                        │        │
│         ▼                                                                                        │        │
│  ┌──────────────────────────────┐                                                              │        │
│  │ ANALYTICS DASHBOARDS         │ ← Metabase, Grafana, Superset, etc.                          │        │
│  │ ├─ User activity reports     │                                                              │        │
│  │ ├─ Content performance       │                                                              │        │
│  │ ├─ Learning patterns         │                                                              │        │
│  │ └─ System health metrics     │                                                              │        │
│  └──────────────────────────────┘                                                              │        │
│                                                                                                  │        │
│ PARALLEL: Archival Process (via Secor)                                                         │        │
│ ├─ Read from druid.events.telemetry                                                            │        │
│ ├─ Compress and batch                                                                           │        │
│ └─ Upload to Cloud Storage (S3, GCS, Azure Blob)                                               │        │
│                                                                                                  │        │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘

END OF PIPELINE
```

---

## Event State Transitions

```
┌─ Initial Event (from Telemetry API)
│  {
│    "mid": "batch-123",
│    "params": { "msgid": "batch-123-unique-id" },
│    "channel": "prod.diksha.portal",
│    "did": "device-456",
│    "events": [
│      {
│        "eid": "CLICK",
│        "edata": { "id": "play-btn", "type": "button" },
│        "object": { "id": "content-789", "type": "Content" }
│      }
│    ]
│  }
│
├─ AFTER Telemetry Extractor
│  {
│    "eid": "CLICK",
│    "edata": { "id": "play-btn", "type": "button" },
│    "object": { "id": "content-789", "type": "Content" },
│    "@timestamp": "2026-04-27T10:30:45.123Z",
│    "syncts": 1745747445123,
│    "flags": {
│      "ex_processed": true,
│      "extractor_duplicate": false
│    }
│  }
│
├─ AFTER Pipeline Preprocessor
│  {
│    ...previous fields...,
│    "flags": {
│      ...previous...,
│      "pp_validation_processed": true,
│      "pp_duplicate": false,
│      "pp_duplicate_skipped": false
│    },
│    "metadata": {
│      "src": "pipeline-preprocessor"
│    }
│  }
│
├─ AFTER De-normalization
│  {
│    ...previous fields...,
│    "devicedata": {
│      "country": "India",
│      "state": "Karnataka",
│      "state_code": "KA",
│      "city": "Bangalore"
│    },
│    "userdata": {
│      "usertype": "teacher",
│      "grade": "8,9",
│      "language": "en",
│      "state": "Karnataka"
│    },
│    "contentdata": {
│      "name": "Algebra Basics",
│      "contentType": "Course",
│      "framework": "NIFT",
│      "subject": "Mathematics",
│      "board": "CBSE",
│      "status": "Live"
│    },
│    "derivedlocationdata": {
│      "location": "Karnataka",
│      "derivedFrom": "user_profile"
│    },
│    "flags": {
│      ...previous...,
│      "denorm_device": true,
│      "denorm_user": true,
│      "denorm_content": true,
│      "denorm_location": true
│    }
│  }
│
└─ AFTER Druid Validator (READY FOR DRUID)
   {
     ...all previous fields...,
     "flags": {
       ...all previous...,
       "dv_duplicate": false
     },
     "validated": true,
     "metadata": {
       ...updated...,
       "job": "druid-validator"
     }
   }
   
   ↓ STORED IN DRUID for Analytics Queries
```

---

## Error Scenarios & Routing

```
SCENARIO 1: Event Size Exceeds Limit
Event Size: 1.5 MB → Exceeds limit (1 MB)
    │
    └─► Sent to: telemetry.failed
        Flags: { ex_processed: false }
        Metadata: { ex_error: "Event size is Exceeded" }
        Analysis: Event too large to process

─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

SCENARIO 2: Duplicate Event Detection
Message ID: "msg-123" → Already in Redis
    │
    └─► Sent to: telemetry.duplicate
        Flags: { extractor_duplicate: true } (or pp_duplicate, or dv_duplicate)
        Count: Incremented duplicate-count metric
        Analysis: Event received multiple times, kept only first

─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

SCENARIO 3: Schema Validation Failure
Event: { eid: "UNKNOWN_EVENT_TYPE", ... }
    │
    └─► Schema file not found for "UNKNOWN_EVENT_TYPE"
        Sent to: telemetry.failed
        Flags: { pp_validation_processed: true (error) }
        Metadata: { pp_validation_error: "Schema not found: eid looks incorrect" }
        Analysis: Event type not recognized

─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

SCENARIO 4: Old Event (> 3 months)
Event Timestamp: 2026-01-01
Current Date: 2026-04-27 (> 3 months old)
    │
    └─► Sent to: telemetry.failed (expired)
        Flags: { event_expired: true }
        Count: Incremented events-expired counter
        Analysis: Event too old, not processed

─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

SCENARIO 5: Redis Cache Miss During Denormalization
Event: { did: "device-xyz" }
Redis Query: redis-meta.device.host:6379 → No data found
    │
    ├─ Action: Continue processing WITHOUT device data
    ├─ Flag: denorm_device: false
    └─ Count: Incremented device-cache-miss counter
    
    Result: Event processed but missing enrichment
    Analysis: Device not in cache (new device or cache expired)

─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─

SCENARIO 6: Redis Connection Error During Denormalization
Redis Query: Connection timeout
    │
    ├─ Action: Job STOPS (fail-fast)
    └─ Exception: JedisException thrown, job fails and restarts
    
    Result: Entire pipeline paused until Redis recovered
    Analysis: Data consistency priority - fail rather than miss enrichment
```

---

## Performance Characteristics

```
THROUGHPUT EXPECTATIONS (per job):

Telemetry Extractor:
  ├─ Input: 10,000 batches/sec (assume 100 events per batch)
  ├─ Output: ~1,000,000 events/sec
  ├─ Processing: ~0.1ms per batch
  └─ Parallelism: 4 consumers × 8 downstream = 32 parallel partitions

Pipeline Preprocessor:
  ├─ Input: ~1,000,000 events/sec
  ├─ Output: ~900,000 valid events/sec (10% filtered/failed)
  ├─ Processing: ~1ms per event (schema validation)
  └─ Parallelism: 4 consumers × 8 downstream = 32 parallel partitions

De-normalization:
  ├─ Input: ~900,000 events/sec
  ├─ Output: ~850,000 enriched events/sec
  ├─ Processing: ~5ms per event (Redis lookups)
  │  ├─ Device cache: 80% hit rate
  │  ├─ User cache: 75% hit rate
  │  ├─ Content cache: 85% hit rate
  │  └─ Location derivation: 90% success
  └─ Parallelism: 4 consumers (primary) + 2 consumers (secondary)

Druid Validator:
  ├─ Input: ~850,000 events/sec
  ├─ Output: ~800,000 validated events/sec
  ├─ Processing: ~0.5ms per event (schema validation only)
  └─ Parallelism: 4 consumers × 8 downstream = 32 parallel partitions

END-TO-END:
  ├─ Latency: ~10-20ms per event (from API to Druid ready)
  ├─ Throughput: ~800,000 events/sec (sustainable)
  └─ Dropoff rate: ~20% events lost due to validation/filtering
```

---

## Key Metrics Dashboard

```
REAL-TIME MONITORING METRICS:

Job: ingest-router
├─ ingest-success-event-count ........... [  8,450/min ] (batches passed through ingest stream)
└─ raw-success-event-count .............. [    100/min ] (events passed through raw stream)

Job: Telemetry Extractor
├─ success-batch-count .................. [  8,450/min ]
├─ failed-batch-count ................... [     15/min ]
├─ success-event-count .................. [  845,000/min ]
├─ failed-event-count ................... [   2,500/min ] (size exceeded)
└─ audit-event-count .................... [  8,450/min ]

Job: Pipeline Preprocessor
├─ validation-success-event-count ....... [  845,000/min ]
├─ validation-failed-event-count ........ [   4,200/min ]
├─ duplicate-event-count ................ [   1,200/min ]
├─ duplicate-skipped-event-count ........ [  100,000/min ] (non-portal)
├─ denorm-primary-route-success-count .. [  650,000/min ]
└─ denorm-secondary-route-success-count  [  195,000/min ]

Job: De-normalization
├─ device-total ......................... [  600,000/min ]
├─ device-cache-hit ..................... [  480,000/min ] (80% hit rate)
├─ device-cache-miss .................... [  120,000/min ] (20% miss rate)
├─ user-total ........................... [  600,000/min ]
├─ user-cache-hit ....................... [  450,000/min ] (75% hit rate)
├─ user-cache-miss ...................... [  150,000/min ] (25% miss rate)
├─ content-total ........................ [  580,000/min ]
├─ content-cache-hit .................... [  493,000/min ] (85% hit rate)
├─ content-cache-miss ................... [   87,000/min ] (15% miss rate)
├─ loc-total ............................ [  600,000/min ]
└─ loc-cache-hit ........................ [  540,000/min ] (90% hit rate)

Job: Druid Validator
├─ validation-success-message-count .... [  850,000/min ]
├─ validation-failed-message-count ..... [    2,000/min ]
├─ telemetry-route-success-count ....... [  800,000/min ]
└─ summary-route-success-count ......... [   50,000/min ]

KAFKA LAG MONITORING:
├─ telemetry.ingestion lag ............. [    < 5ms  ] ← ingest-router-group (FIRST alert)
├─ telemetry.ingest lag ................ [    < 10ms ] ← telemetry-extractor input
├─ telemetry.raw lag ................... [    < 20ms ]
├─ telemetry.unique lag ................ [   < 100ms ] (denorm is slower)
├─ druid.events.telemetry lag .......... [    < 50ms ]
└─ Overall end-to-end latency .......... [   < 500ms ]

NOTE: ingest-router-group lag is the earliest-in-pipeline observable signal.
A lag spike here means the problem is upstream (SDK burst or Kafka broker).
A lag spike only on telemetry.ingest means ingest-router is the bottleneck.
A lag spike only on telemetry.raw means telemetry-extractor is the bottleneck.
```

---

**End of Sequence Diagram**
