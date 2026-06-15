# Sunbird Data Pipeline - Architecture & Flink Jobs Guide

**Last Updated:** April 27, 2026

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow Diagram](#data-flow-diagram)
3. [Key Concepts](#key-concepts)
4. [Job-by-Job Deep Dive](#job-by-job-deep-dive)
   - [0. Ingest Router](#0-ingest-router)
   - [1. Telemetry Extractor](#1-telemetry-extractor)
   - [2. Pipeline Preprocessor](#2-pipeline-preprocessor)
   - [3. De-normalization](#3-de-normalization)
   - [4. Druid Events Validator](#4-druid-events-validator)
5. [Technology Stack](#technology-stack)
6. [Data Quality & Resilience](#data-quality--resilience)

---

## Architecture Overview

### High-Level Purpose
The Sunbird Data Pipeline is a streaming data processing system that captures, validates, deduplicates, enriches, and stores telemetry events from the Diksha/Sunbird platform. It processes clickstream data from the Telemetry API through a series of Flink jobs, with each job performing specific transformations before final storage in Druid (analytics database).

### Key Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TELEMETRY API                                       │
│                    (Collects user interactions)                             │
└──────────────────────────────┬──────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────────────────────┐
        │       KAFKA TOPIC: telemetry.ingestion               │
        │  (External entry point — SDK/API writes here)        │
        └───────────────────┬─────────────────────────────────┘
                           │
        ┌──────────────────▼──────────────────────────────────┐
        │   FLINK JOB: ingest-router                          │
        │   - Pure pass-through (no transformation)           │
        │   - Decouples SDK write contract from pipeline      │
        │   - Also routes: telemetry.raw.event → telemetry.raw│
        └──────────────────┬──────────────────────────────────┘
                           │
        ┌──────────────────▼───────────────────────────────────┐
        │       KAFKA TOPIC: telemetry.ingest                  │
        │  (Internal pipeline entry — batch events)           │
        └───────────────────┬─────────────────────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │   FLINK JOB: telemetry-extractor    │
        │   - Deduplication                   │
        │   - Batch unpacking                 │
        │   - Audit event generation          │
        └──────────────────┬──────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │    KAFKA: telemetry.raw             │
        │    (Unpacked, unique events)        │
        └──────────────────┬──────────────────┘
                           │
        ┌──────────────────▼──────────────────────────┐
        │  FLINK JOB: pipeline-preprocessor           │
        │  - Schema validation                        │
        │  - Data correction                          │
        │  - Deduplication (portal/desktop only)      │
        │  - Event routing & flattening               │
        └──────────────────┬──────────────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │    KAFKA: telemetry.unique          │
        │    (Validated, unique events)       │
        └──────────────────┬──────────────────┘
                           │
        ┌──────────────────▼──────────────────────┐
        │   FLINK JOB: de-normalization           │
        │   - Device enrichment                    │
        │   - User enrichment                      │
        │   - Content enrichment                   │
        │   - Location derivation                  │
        └──────────────────┬──────────────────────┘
                           │
        ┌──────────────────▼────────────────────────┐
        │    KAFKA: telemetry.unique.primary        │
        │    telemetry.unique.secondary             │
        │    (Denormalized, split for validation)   │
        └──────────────────┬────────────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │  FLINK JOB: druid-events-validator   │
        │  - Final validation                  │
        │  - Summary deduplication             │
        │  - Event routing                     │
        └──────────────────┬──────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │    KAFKA: druid.events.telemetry    │
        │    (Final, validated events)        │
        └──────────────────┬──────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │         DRUID DATABASE               │
        │    (Analytics data warehouse)       │
        │  ├─ Primary events                   │
        │  ├─ Summary events                   │
        │  ├─ Log events                       │
        │  └─ Error events                     │
        └──────────────────────────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │       CLOUD STORAGE (Archive)       │
        │    (Secor + Cloud Storage)          │
        └──────────────────────────────────────┘
```

### External Dependencies

- **Redis**: Used for caching and deduplication
  - User Cache (User metadata)
  - Content Cache (Content metadata)
  - Device Cache (Device information)
  - DialCode Cache (QR codes)
  - Deduplication Store (Message IDs)
  
- **Kafka**: Message broker for inter-job communication
- **Druid**: Analytics database for final storage

---

## Data Flow Diagram

```
ENTRY POINT: telemetry.ingestion  (SDK/API external write target)
    │
    ├─► ingest-router ──────────► Pure pass-through, zero transformation
    │                             Bridges external contract → internal pipeline
    │
    │   KAFKA: telemetry.ingest  (internal pipeline entry point)
    │   Contains: mid (batch ID), params (msgid), channel, did, events[]
    │
    ├─► telemetry-extractor ────► Deduplicates batch by msgid
    │                             │
    │                             ├─► telemetry.raw (unique)
    │                             └─► duplicate topic (if duplicate)
    │
    ├─► pipeline-preprocessor ───► Validates event schema
    │                              │ Corrects data issues
    │                              │ Deduplicates by mid (portal/desktop only)
    │                              │ Routes by event type
    │                              │
    │                              ├─► telemetry.unique (primary route)
    │                              ├─► telemetry.log (log events)
    │                              ├─► telemetry.audit (audit events)
    │                              ├─► failed topic (validation failed)
    │                              └─► duplicate topic (duplicates)
    │
    ├─► de-normalization ────────► Enriches with Redis caches:
    │                              │ - Device data (geo, specs)
    │                              │ - User data (profile)
    │                              │ - Content data (metadata)
    │                              │ - DialCode data (QR info)
    │                              │ - Derived location
    │                              │
    │                              ├─► telemetry.unique.primary
    │                              └─► telemetry.unique.secondary
    │
    ├─► druid-events-validator ──► Final validation
    │                              │ Deduplicates summaries
    │                              │
    │                              ├─► druid.events.telemetry (valid)
    │                              ├─► summary route (summary events)
    │                              ├─► log route (log events)
    │                              ├─► error route (error events)
    │                              ├─► failed topic (validation failed)
    │                              └─► duplicate topic (duplicates)
    │
    └─► Druid + Cloud Storage
```

---

## Key Concepts

### Deduplication

**What it is:** Prevents duplicate events from being processed multiple times.

**How it works:**
- Uses Redis to store message IDs
- First occurrence of a message ID → marked as **unique** (stored in Redis)
- Second occurrence → marked as **duplicate** (sent to duplicate topic)
- Expires after configured TTL (e.g., 24 hours)

**Usage:**
- **Batch Level** (Telemetry Extractor): Uses `msgid` from batch params
- **Event Level** (Pipeline Preprocessor): Uses `mid` (message ID) from event
- **Summary Level** (Druid Validator): Deduplicates summary events

### Denormalization (Data Enrichment)

**What it is:** Adding related data from Redis caches to the event.

**What data is added:**

| Cache Type | What it provides | Example |
|-----------|-----------------|---------|
| **Device** | Device info, geo data, state/city codes | Country, state, city, device specs |
| **User** | User profile data | User type, grade, language, state, district |
| **Content** | Content metadata | Name, content type, framework, subject, board |
| **DialCode** | QR code information | Channel, publisher, status, dates |
| **Location** | Derived location with fallback | User profile location > User declared > IP location |

**Why it matters:** Enables analytics queries without multiple lookups; all relevant data is in one place.

### Event Types & Routing

Events are categorized and routed to different topics based on type:

| Event Type | Description | Routing |
|-----------|-------------|---------|
| **TELEMETRY** | Regular user interaction events (VIEW, CLICK, etc.) | telemetry router topic |
| **SUMMARY** | Aggregated summary events (ME_WORKFLOW_SUMMARY, etc.) | summary router topic |
| **LOG** | System log events | log router topic |
| **ERROR** | Error/exception events | error router topic |
| **AUDIT** | Audit trail events | audit topic |
| **ASSESS** | Assessment events (may need redaction) | assess.raw topic |

### Flags & Metadata

Each event is marked with **flags** and **metadata** to track processing history:

```json
{
  "flags": {
    "ex_processed": true,           // Extractor processing
    "pp_validation_processed": true, // Pipeline preprocessor validation
    "pp_duplicate": false,           // Not a duplicate
    "pp_duplicate_skipped": false,   // Dedup not skipped
    "dv_duplicate": false            // Druid validator dedup
  },
  "metadata": {
    "src": "pipeline-preprocessor",  // Source job
    "pp_validation_error": null      // Any validation errors
  }
}
```

---

## Job-by-Job Deep Dive

---

## 0. Ingest Router

**Location:** `data-pipeline-flink/ingest-router/`

**Role:** Pass-through gateway that separates the external SDK/API write contract from the internal pipeline. Runs two independent byte-stream pipelines with zero transformation logic. It is the true entry point of the data pipeline — telemetry-extractor never sees raw SDK traffic directly.

### 0.1 Input/Output

| Stream | Input Topic | Output Topic | Consumer Group |
|--------|------------|-------------|----------------|
| **Ingest stream** | `{env}.telemetry.ingestion` | `{env}.telemetry.ingest` | `{env}-ingest-router-group` |
| **Raw stream** | `{env}.telemetry.raw.event` | `{env}.telemetry.raw` | `{env}-ingest-router-group` |

### 0.2 Processing Flow

```
External Producer (SDK / Telemetry API)
    │
    │  Writes batch events (bytes)
    ▼
[telemetry.ingestion]  ◄── SDK/API target topic
    │
    │  kafkaBytesSource → kafkaBytesSink (no deserialization)
    ▼
FLINK JOB: ingest-router (Ingest Stream)
    │  parallelism: task.consumer.parallelism
    │  No ProcessFunction — direct source → sink
    ▼
[telemetry.ingest]  ◄── Pipeline internal entry
    │
    ▼
  telemetry-extractor (reads from here)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Parallel: Raw Stream (independent path)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[telemetry.raw.event]  ◄── Pre-formatted individual events (non-batch path)
    │
    │  kafkaBytesSource → kafkaBytesSink (no deserialization)
    ▼
FLINK JOB: ingest-router (Raw Stream)
    │  parallelism: task.raw.consumer.parallelism
    ▼
[telemetry.raw]  ◄── pipeline-preprocessor input (same topic as extractor output)
```

### 0.3 Why This Job Exists

**1. External/internal topic name decoupling**
SDKs, mobile apps, and the Telemetry API are configured to write to `telemetry.ingestion`. The internal pipeline processes from `telemetry.ingest`. The router is the contractual seam — either side can be renamed or reconfigured independently without touching the other.

**2. Flink-managed offset boundary**
By routing through a named Flink consumer group (`{env}-ingest-router-group`), the pipeline gains Flink's checkpoint-based offset commit semantics at the ingestion boundary. Consumer lag on this group is the primary early-warning signal that the pipeline is falling behind SDK write volume.

**3. Dedicated monitoring point**
The job publishes `ingest-success-event-count` and `raw-success-event-count` as named Flink metrics. Kubernetes Helm alert rules fire on consumer group lag for this group specifically — providing a clean, SDK-independent view of ingestion health.

**4. Parallelism isolation**
The Kubernetes deployment spec allocates independent parallelism for the ingest stream and raw stream, allowing throughput tuning per path without touching downstream jobs.

**5. Raw event ingestion path**
The raw stream (`telemetry.raw.event` → `telemetry.raw`) serves a secondary ingestion path for events that are already individually formatted rather than batch-wrapped. These events merge into the same `telemetry.raw` topic that `telemetry-extractor` writes to, entering the pipeline at the preprocessor stage.

### 0.4 Key Implementation Details

```scala
// IngestRouterStreamTask.scala — the entire job logic
def process(): Unit = {
  // Ingest stream: bytes in → bytes out, no transformation
  env.addSource(kafkaConnector.kafkaBytesSource(config.kafkaIngestInputTopic),
                config.telemetryIngestProcessorConsumer)
    .addSink(kafkaConnector.kafkaBytesSink(config.kafkaIngestSuccessTopic))

  // Raw stream: same pattern, independent parallelism
  env.addSource(kafkaConnector.kafkaBytesSource(config.kafkaRawInputTopic),
                config.telemetryRawProcessorConsumer)
    .addSink(kafkaConnector.kafkaBytesSink(config.kafkaRawSuccessTopic))

  env.execute(config.jobName)
}
```

No `ProcessFunction`, no deserialization, no business logic. The job is intentionally minimal by design.

### 0.5 Configuration

```properties
# Ingest stream
kafka.ingest.input.topic  = {env}.telemetry.ingestion
kafka.ingest.output.success.topic = {env}.telemetry.ingest
task.consumer.parallelism = 1
task.downstream.operators.parallelism = 1

# Raw stream
kafka.raw.input.topic = {env}.telemetry.raw.event
kafka.raw.output.success.topic = {env}.telemetry.raw
task.raw.consumer.parallelism = 1
task.raw.downstream.operators.parallelism = 1
```

### 0.6 Metrics Tracked

| Metric | Meaning |
|--------|---------|
| `ingest-success-event-count` | Messages routed through ingest stream |
| `raw-success-event-count` | Messages routed through raw stream |

**Kubernetes deployment:**
- 2 replicas, 2 task slots each
- CPU: 1 request / 2 limit
- Memory: 2Gi per pod (1700m taskmanager, 1600m jobmanager process memory)

---

## 1. Telemetry Extractor

**Location:** `data-pipeline-flink/telemetry-extractor/`

**Role:** Extract individual events from batch, deduplicate batches, and prepare for processing.

### 1.1 Input/Output

| Aspect | Details |
|--------|---------|
| **Input Topic** | `telemetry.ingest` |
| **Input Format** | Batch events (JSON): `{ mid, params, channel, did, events: [] }` |
| **Output Topics** | `telemetry.raw`, `telemetry.assess.raw`, `duplicate`, `failed` |
| **Output Format** | Individual events (Map<String, AnyRef>) |

### 1.2 Processing Flow

```
Input Batch Event
    │
    ├─► DeduplicationFunction
    │   ├─ Extract msgid from params.msgid
    │   ├─ Check Redis for msgid existence
    │   ├─ If exists → send to duplicate topic
    │   └─ If not → continue
    │
    ├─► ExtractionFunction
    │   ├─ Extract events[] from batch
    │   ├─ For each event:
    │   │   ├─ Update syncts and @timestamp
    │   │   ├─ Check event size (< 1 MB)
    │   │   ├─ If too large → send to failed topic
    │   │   └─ If OK → route based on event type:
    │   │       ├─ If ASSESS/RESPONSE → assess-redact-events tag
    │   │       ├─ If LOG → log-events tag
    │   │       └─ Else → raw-events tag
    │   │
    │   └─ Generate audit event with event count
    │
    └─► Output to Kafka topics
```

### 1.3 Key Functions

#### **DeduplicationFunction**

**Purpose:** Remove duplicate batch events (same msgid)

```scala
// Located in: DeduplicationFunction.scala

def processElement(batchEvents: String, context, metrics): Unit = {
  val msgId = getMsgIdentifier(batchEvents)  // Extract msgid from params
  
  deDup[String, util.Map[String, AnyRef]](
    msgId,
    batchEvents,
    context,
    config.uniqueEventOutputTag,      // Output tag for unique
    config.duplicateEventOutputTag,   // Output tag for duplicates
    flagName = "extractor_duplicate"  // Flag to mark duplicates
  )(dedupEngine, metrics)
}

// Behind the scenes in DedupEngine:
// 1. Check if msgId exists in Redis
// 2. If NOT → Add to Redis, mark as unique, send to unique output tag
// 3. If YES → Mark as duplicate, send to duplicate output tag
```

**Metrics Tracked:**
- `success-batch-count` - Unique batches processed
- `failed-batch-count` - Batches with processing errors

#### **ExtractionFunction**

**Purpose:** Unpack events from batch and route based on type

```scala
// Located in: ExtractionFunction.scala

def processElement(batchEvent: util.Map[String, AnyRef], context, metrics): Unit = {
  val eventsList = getEventsList(batchEvent)  // Extract events[]
  val syncTs = Option(batchEvent.get("syncts"))
    .getOrElse(System.currentTimeMillis()).asInstanceOf[Number].longValue()
  
  eventsList.forEach(event => {
    val eventId = event.get("eid").asInstanceOf[String]  // Event ID (CLICK, VIEW, etc.)
    val eventData = updateEvent(event, syncTs)            // Add @timestamp, syncts
    val eventJson = JSONUtil.serialize(eventData)
    val eventSize = eventJson.getBytes("UTF-8").length    // Check size
    
    if (eventSize > config.eventMaxSize) {
      // Event too large
      metrics.incCounter(config.failedEventCount)
      context.output(config.failedEventsOutputTag, markFailed(eventData))
    } else {
      metrics.incCounter(config.successEventCount)
      if (config.redactEventsList.contains(eventId)) {
        // Assessment event - needs redaction
        context.output(config.assessRedactEventsOutputTag, markSuccess(eventData))
      } else if ("LOG".equalsIgnoreCase(eventId)) {
        // Log event
        context.output(config.logEventsOutputTag, markSuccess(eventData))
      } else {
        // Regular event
        context.output(config.rawEventsOutputTag, markSuccess(eventData))
      }
    }
  })
  
  // Generate audit event
  context.output(config.auditEventsOutputTag, 
    generateAuditEvents(eventsList.size(), batchEvent))
}
```

**Event Size Check:**
- Max size: 1 MB (configurable)
- If exceeded → Event sent to failed topic with `ex_processed=false` flag

**Audit Event Generation:**
- Creates a LOG event with batch metadata
- Contains: mid, channel, did, event count, consumer ID
- Helps track batch processing statistics

**Metrics Tracked:**
- `success-event-count` - Events extracted successfully
- `failed-event-count` - Events exceeding size limit
- `audit-event-count` - Audit events generated

### 1.4 Output Tags (Side Outputs)

```scala
rawEventsOutputTag             → telemetry.raw (majority of events)
logEventsOutputTag             → telemetry.log (LOG events)
errorEventsOutputTag           → telemetry.error (ERROR events)
assessRedactEventsOutputTag    → assess-redact-events (ASSESS/RESPONSE - for redaction)
assessRawEventsOutputTag       → telemetry.assess.raw (after redaction)
failedEventsOutputTag          → telemetry.failed (size exceeded)
failedBatchEventOutputTag      → telemetry.batch.failed (batch processing errors)
auditEventsOutputTag           → telemetry.audit (audit events)
duplicateEventOutputTag        → telemetry.duplicate (duplicate batches)
uniqueEventOutputTag           → telemetry.unique (for dedup pass-through)
```

### 1.5 Configuration

```properties
# Input/Output Topics
kafka.input.topic = telemetry.ingest
kafka.output.success.topic = telemetry.raw
kafka.output.assess.raw.topic = telemetry.assess.raw
kafka.output.duplicate.topic = telemetry.duplicate
kafka.output.failed.topic = telemetry.failed

# Event Size Limit
kafka.event.max.size = 1048576  # 1 MB

# Processing
task.consumer.parallelism = 4
task.downstream.operators.parallelism = 8

# Deduplication
redis.database.duplicationstore.id = 0
redis.database.key.expiry.seconds = 86400  # 24 hours

# Events to Redact
redact.events.list = [ASSESS, RESPONSE]
```

---

## 2. Pipeline Preprocessor

**Location:** `data-pipeline-flink/pipeline-preprocessor/`

**Role:** Validate events against schema, correct data issues, deduplicate, and route events.

### 2.1 Input/Output

| Aspect | Details |
|--------|---------|
| **Input Topic** | `telemetry.raw` |
| **Input Format** | Individual events (Event domain object) |
| **Output Topics** | `telemetry.unique`, `telemetry.duplicate`, `telemetry.failed`, `telemetry.audit`, `telemetry.log` |
| **Secondary Routes** | `denorm.primary`, `denorm.secondary` (for denormalization) |

### 2.2 Processing Flow

```
Input Event (from telemetry.raw)
    │
    ├─► TelemetryValidationFunction
    │   ├─ Data Correction:
    │   │   ├─ Remove federated user ID prefix ("f:" → original ID)
    │   │   ├─ Correct dialCode key (if SEARCH event)
    │   │   ├─ Correct dialCode value (if DialCode/QR object)
    │   │   └─ Set default channel if missing
    │   │
    │   ├─ Schema Validation:
    │   │   ├─ Check if schema exists for event type (eid)
    │   │   ├─ If not → send to failed topic
    │   │   └─ If yes → validate against schema
    │   │
    │   ├─ On Validation Success:
    │   │   ├─ Mark: pp_validation_processed=true
    │   │   ├─ Check if dedup required (config.includedProducersForDedup)
    │   │   │   ├─ If YES → proceed to deduplication
    │   │   │   └─ If NO → mark pp_duplicate_skipped=true
    │   │   └─ Send to unique-events output tag
    │   │
    │   └─ On Validation Failure:
    │       ├─ Mark: pp_validation_processed=true (with error)
    │       ├─ Record error message
    │       └─ Send to failed topic
    │
    ├─► DeduplicationFunction
    │   ├─ Extract mid (message ID)
    │   ├─ Check Redis for mid
    │   ├─ If exists → mark duplicate, send to duplicate topic
    │   └─ If not → add to Redis, send to unique-events output tag
    │
    ├─► TelemetryRouterFunction
    │   ├─ Route PRIMARY events:
    │   │   ├─ Regular events → denorm.primary
    │   │   ├─ Share events → separate stream for flattening
    │   │   └─ Audit events → denorm.primary + audit topic
    │   │
    │   ├─ Route SECONDARY (low priority):
    │   │   └─ Certain event types → denorm.secondary
    │   │
    │   ├─ Route SPECIAL events:
    │   │   ├─ LOG events → log topic (skip denorm)
    │   │   ├─ ERROR events → error topic (skip denorm)
    │   │   └─ CB_AUDIT events → cb.audit topic
    │   │
    │   └─ OUTPUT: denorm.primary, denorm.secondary, audit, log, error topics
    │
    ├─► ShareEventsFlattenerFunction (if SHARE event)
    │   ├─ If transfers = 0 → type = "download"
    │   ├─ If transfers > 0 → type = "import"
    │   ├─ Move object data to rollup L1
    │   └─ Create individual SHARE_ITEM events
    │
    └─► Final Output to Kafka
```

### 2.3 Key Functions

#### **TelemetryValidationFunction**

**Purpose:** Validate events against JSON schema and apply data corrections

```scala
// Located in: TelemetryValidationFunction.scala

def processElement(event: Event, context, metrics): Unit = {
  val isSchemaPresent = schemaValidator.schemaFileExists(event)
  
  if (!isSchemaPresent) {
    onMissingSchema(event, metrics, context, "Schema not found")
  } else {
    val validationReport = schemaValidator.validate(event, isSchemaPresent)
    if (validationReport.isSuccess) {
      onValidationSuccess(event, metrics, context)
    } else {
      onValidationFailure(event, metrics, context, validationReport)
    }
  }
}

def dataCorrection(event: Event): Event = {
  // Remove federated user ID prefix
  val eventActorId = event.actorId()
  if (eventActorId != null && eventActorId.startsWith("f:")) {
    // "f:org1:user123" → "user123"
    event.updateActorId(eventActorId.substring(eventActorId.lastIndexOf(":") + 1))
  }
  
  // Correct dialCode key for SEARCH events
  if (event.eid().equalsIgnoreCase("SEARCH")) {
    event.correctDialCodeKey()  // dialCodes → dialcode
  }
  
  // Correct dialCode value for DialCode/QR objects
  if (event.objectFieldsPresent && 
      (event.objectType().equalsIgnoreCase("DialCode") || 
       event.objectType().equalsIgnoreCase("qr"))) {
    event.correctDialCodeValue()
  }
  
  event
}

def onValidationSuccess(event: Event, metrics, context): Unit = {
  dataCorrection(event)
  event.markSuccess(config.VALIDATION_FLAG_NAME)
  metrics.incCounter(config.validationSuccessMetricsCount)
  event.updateDefaults(config)  // Set default channel, etc.
  
  // Check if dedup required for this producer
  if (isDuplicateCheckRequired(event.producerId())) {
    // Dedup for portal/desktop only
    deDup[Event, Event](event.mid(), event, context, 
      config.uniqueEventsOutputTag, 
      config.duplicateEventsOutputTag, 
      flagName = config.DEDUP_FLAG_NAME)(dedupEngine, metrics)
  } else {
    // Skip dedup for other producers
    event.markSkipped(config.DEDUP_SKIP_FLAG_NAME)
    context.output(config.uniqueEventsOutputTag, event)
    metrics.incCounter(uniqueEventMetricCount)
  }
}
```

**Data Corrections Applied:**
1. **Federated User ID**: Remove organization prefix
2. **Channel**: Set to default if missing (default: "org.sunbird")
3. **Timestamps**: Update syncTs and @timestamp if missing
4. **DialCode**: Correct spelling inconsistencies
5. **Default values**: Apply config defaults

**Schema Validation:**
- Uses JSON Schema files stored in configured path
- Different schemas for different event types (eid)
- Returns list of validation failures if schema check fails

**Metrics Tracked:**
- `validation-success-event-count` - Valid events
- `validation-failed-event-count` - Failed validation
- `duplicate-event-count` - Duplicate events
- `duplicate-skipped-event-count` - Events where dedup skipped

#### **TelemetryRouterFunction**

**Purpose:** Route validated events to appropriate downstream topics/streams

```scala
// Located in: TelemetryRouterFunction.scala

def processElement(event: Event, context, metrics): Unit = {
  
  // Check for audit events
  if (event.isAuditEvent()) {
    metrics.incCounter(config.auditEventRouterMetricCount)
    context.output(config.auditRouteEventsOutputTag, event)
    // Also send to audit topic
  }
  
  // Check for CB audit events
  if (event.isCBauditEvent()) {
    metrics.incCounter(config.cbAuditEventRouterMetricCount)
    context.output(config.cbAuditRouteEventsOutputTag, event)
  }
  
  // Check for log events (skip denorm)
  if (event.isLogEvent()) {
    metrics.incCounter(config.logEventsRouterMetricsCount)
    context.output(config.logEventsOutputTag, event)
    return  // Don't denormalize log events
  }
  
  // Check for error events (skip denorm)
  if (event.isErrorEvent()) {
    metrics.incCounter(config.errorEventsRouterMetricsCount)
    context.output(config.errorEventOutputTag, event)
    return  // Don't denormalize error events
  }
  
  // Check for share events (special processing)
  if (event.isShareEvent()) {
    context.output(config.shareRouteEventsOutputTag, event)
    return  // Send to ShareEventsFlattener
  }
  
  // Regular telemetry events - route to denorm
  val isPrimaryEvent = isPrimaryRouteEvent(event)
  if (isPrimaryEvent) {
    metrics.incCounter(config.denormPrimaryEventsRouterMetricsCount)
    context.output(config.denormPrimaryEventsRouteOutputTag, event)
  } else {
    metrics.incCounter(config.denormSecondaryEventsRouterMetricsCount)
    context.output(config.denormSecondaryEventsRouteOutputTag, event)
  }
}

def isPrimaryRouteEvent(event: Event): Boolean = {
  // Primary route = higher priority events (faster denorm processing)
  val primaryEventTypes = List("CLICK", "VIEW", "ASSESS", "RESPONSE", "INTERACT")
  primaryEventTypes.contains(event.eid())
}
```

**Routing Decision Matrix:**

| Event Type | Action | Output Topic |
|-----------|--------|--------------|
| **AUDIT** | Route + Send to audit | denorm.primary + audit |
| **CB_AUDIT** | Route | cb.audit |
| **LOG** | Skip denorm | log topic |
| **ERROR** | Skip denorm | error topic |
| **SHARE** | Send to flattener | ShareEventsFlattener stream |
| **Regular** (priority) | Send to denorm | denorm.primary |
| **Regular** (low priority) | Send to denorm | denorm.secondary |

#### **ShareEventsFlattenerFunction**

**Purpose:** Flatten SHARE events into individual SHARE_ITEM events

```scala
// Located in: ShareEventsFlattenerFunction.scala

def processElement(event: Event, context, metrics): Unit = {
  if (event.isShareEvent()) {
    val shareItems = event.getShareItems()  // Get items array
    
    shareItems.forEach(item => {
      // Determine type based on transfers count
      val transfersCount = item.getTransfers().getOrElse(0)
      val type = if (transfersCount == 0) "download" else "import"
      
      // Create individual SHARE_ITEM event
      val shareItemEvent = createShareItemEvent(event, item, type)
      
      // Move object data to rollup L1 if present
      if (event.hasObject()) {
        shareItemEvent.setRollupL1(event.getObject())
      }
      
      // Output individual SHARE_ITEM event
      context.output(config.shareItemEventOutputTag, shareItemEvent)
      metrics.incCounter(config.shareItemEventsMetircsCount)
    })
    
    // Also output original SHARE event
    context.output(config.shareRouteEventsOutputTag, event)
    metrics.incCounter(config.shareEventsRouterMetricCount)
  }
}
```

**Share Event Processing:**
- **transfers = 0** → Type = "download" (user downloaded content)
- **transfers > 0** → Type = "import" (user imported/shared with others)
- Duplicates object data into rollup for easier querying
- Creates separate events for analytics

### 2.4 Output Tags (Side Outputs)

```scala
validationFailedEventsOutputTag    → telemetry.failed
uniqueEventsOutputTag              → unique-events (to router)
duplicateEventsOutputTag           → telemetry.duplicate
primaryRouteEventsOutputTag        → primary-route (to denorm)
denormSecondaryEventsRouteOutputTag → denorm.secondary (to denorm)
denormPrimaryEventsRouteOutputTag  → denorm.primary (to denorm)
auditRouteEventsOutputTag          → telemetry.audit
cbAuditRouteEventsOutputTag        → telemetry.cb.audit
logEventsOutputTag                 → telemetry.log
errorEventOutputTag                → telemetry.error
shareRouteEventsOutputTag          → share-route (to flattener)
shareItemEventOutputTag            → telemetry.denorm (flattened items)
```

### 2.5 Configuration

```properties
# Input/Output Topics
kafka.input.topic = telemetry.raw
kafka.output.primary.route.topic = telemetry.denorm
kafka.output.audit.route.topic = telemetry.audit
kafka.output.log.route.topic = telemetry.log
kafka.output.error.route.topic = telemetry.error
kafka.output.failed.topic = telemetry.failed
kafka.output.duplicate.topic = telemetry.duplicate

# Denorm split topics
kafka.output.denorm.primary.route.topic = telemetry.denorm.primary
kafka.output.denorm.secondary.route.topic = telemetry.denorm.secondary

# Schema & Validation
telemetry.schema.path = /schemas/telemetry/
default.channel = org.sunbird

# Deduplication
dedup.producer.included.ids = [prod.diksha.portal, prod.sunbird.desktop]
redis.database.duplicationstore.id = 0
redis.database.key.expiry.seconds = 86400

# Processing
task.consumer.parallelism = 4
task.downstream.operators.parallelism = 8
```

---

## 3. De-normalization

**Location:** `data-pipeline-flink/de-normalization/`

**Role:** Enrich events with metadata from Redis caches (device, user, content, location).

### 3.1 Input/Output

| Aspect | Details |
|--------|---------|
| **Input Topics** | `denorm.primary`, `denorm.secondary` |
| **Input Format** | Events with required IDs (did, actor.id, object.id) |
| **Output Topics** | `telemetry.unique.primary`, `telemetry.unique.secondary`, `telemetry.failed` |
| **Output Format** | Enriched events with devicedata, userdata, contentdata, dialcodedata |

### 3.2 Processing Flow

```
Input Event (from denorm.primary or denorm.secondary)
    │
    ├─► Event Age Check
    │   ├─ If older than ignorePeriodInMonths (default: 3)
    │   │   ├─ Send to failed topic
    │   │   └─ Increment eventsExpired counter
    │   └─ Continue if recent
    │
    ├─► Event Type Filter
    │   ├─ If SUMMARY (except ME_WORKFLOW_SUMMARY) or in eventsToskip list
    │   │   └─ Skip denorm, increment eventsSkipped
    │   └─ Continue for regular events
    │
    ├─► Denormalization Pipeline
    │   │
    │   ├─► DeviceDenormalization
    │   │   ├─ If event has 'did' (device ID)
    │   │   │   ├─ Fetch device data from Redis (device cache)
    │   │   │   ├─ Extract: country, state, city, devicespec, firstaccess, etc.
    │   │   │   ├─ Derive ISO state code from state
    │   │   │   ├─ Add under event.devicedata
    │   │   │   └─ Track: did-total, did-cache-hit, did-cache-miss
    │   │   └─ Continue if no 'did'
    │   │
    │   ├─► UserDenormalization
    │   │   ├─ If event has 'actor.id' and actor.type = 'User'
    │   │   │   ├─ Fetch user data from Redis (user cache)
    │   │   │   ├─ Extract: usertype, grade, language, state, district, etc.
    │   │   │   ├─ Add under event.userdata
    │   │   │   └─ Track: user-total, user-cache-hit, user-cache-miss
    │   │   └─ Continue if no actor.id or actor.type != User
    │   │
    │   ├─► DialcodeDenormalization
    │   │   ├─ If object.type = 'dialcode' or 'qr'
    │   │   │   ├─ Fetch dialcode data from Redis
    │   │   │   ├─ Extract: channel, publisher, generated_on, published_on, status
    │   │   │   ├─ Convert timestamps to epoch
    │   │   │   ├─ Add under event.dialcodedata
    │   │   │   └─ Track: dialcode-total, dialcode-cache-hit, dialcode-cache-miss
    │   │   └─ Continue if no dialcode object
    │   │
    │   ├─► ContentDenormalization
    │   │   ├─ If object.type != 'dialcode', 'qr', or 'user'
    │   │   │   ├─ Fetch content data from Redis
    │   │   │   ├─ Extract: name, contentType, framework, subject, board, etc.
    │   │   │   ├─ Add under event.contentdata
    │   │   │   ├─ If object.rollup.l1 exists and != object.id
    │   │   │   │   ├─ Fetch collection data from Redis
    │   │   │   │   └─ Add under event.collectiondata
    │   │   │   ├─ Convert timestamps to epoch
    │   │   │   └─ Track: content-total, content-cache-hit, content-cache-miss
    │   │   └─ Continue if no object or different type
    │   │
    │   ├─► LocationDenormalization
    │   │   ├─ Derive location with fallback priority:
    │   │   │   1. User profile location (from userdata)
    │   │   │   2. User declared location (from devicedata)
    │   │   │   3. IP location (from devicedata)
    │   │   ├─ Add under event.derivedlocationdata
    │   │   │   ├─ location (final derived value)
    │   │   │   └─ derivedFrom (source indicator)
    │   │   └─ Track: loc-total, loc-cache-hit, loc-cache-miss
    │   │
    │   └─► Flag Event with Denorm Status
    │       └─ Mark which denorm operations completed successfully
    │
    └─► Output to Kafka
        ├─ telemetry.unique.primary (from denorm.primary input)
        └─ telemetry.unique.secondary (from denorm.secondary input)
```

### 3.3 Key Functions

#### **DenormalizationFunction**

**Purpose:** Orchestrate denormalization from multiple Redis caches

```scala
// Located in: DenormalizationFunction.scala

def processElement(event: Event, context, metrics): Unit = {
  // Check if event is older than 3 months
  if (event.isOlder(config.ignorePeriodInMonths)) {
    metrics.incCounter(config.eventsExpired)
    return
  }
  
  // Skip denorm for specific summary events
  if ("ME_WORKFLOW_SUMMARY" != event.eid() && 
      (event.eid().contains("SUMMARY") || config.eventsToskip.contains(event.eid()))) {
    metrics.incCounter(config.eventsSkipped)
    return
  }
  
  // Fetch all necessary data from Redis
  val cacheData = denormCache.getDenormData(event)
  
  // Apply denormalizations
  deviceDenormalization.denormalize(event, cacheData, metrics)
  userDenormalization.denormalize(event, cacheData, metrics)
  dialcodeDenormalization.denormalize(event, cacheData, metrics)
  contentDenormalization.denormalize(event, cacheData, metrics)
  locationDenormalization.denormalize(event, metrics)
  
  // Output enriched event
  context.output(config.denormEventsTag, event)
}
```

#### **DeviceDenormalization**

**Purpose:** Add device metadata to events

```scala
def denormalize(event: Event, cacheData: DenormCache, metrics: Metrics): Unit = {
  val deviceId = event.deviceId()  // Extract 'did'
  
  if (deviceId != null && !deviceId.isEmpty) {
    metrics.incCounter(config.deviceTotal)
    
    try {
      // Try to fetch from Redis
      val deviceData = denormCache.getDeviceData(deviceId)
      
      if (deviceData != null) {
        metrics.incCounter(config.deviceCacheHit)
        // Extract configured fields
        val fields = Map(
          "country" → deviceData.get("country"),
          "state" → deviceData.get("state"),
          "state_code" → deviceData.get("state_code"),
          "city" → deviceData.get("city"),
          "district" → deviceData.get("district_custom"),
          "devicespec" → deviceData.get("devicespec"),
          "firstaccess" → deviceData.get("firstaccess")
        )
        
        // Add to event
        event.addDeviceData(fields)
      } else {
        metrics.incCounter(config.deviceCacheMiss)
        // Log warning but continue
        logger.warn(s"Device cache miss for ${deviceId}")
      }
    } catch {
      case e: Exception ⇒
        metrics.incCounter(config.deviceCacheMiss)
        logger.error(s"Error fetching device data: ${e.getMessage}", e)
        throw e  // Stop job on Redis error
    }
  }
}
```

#### **UserDenormalization**

**Purpose:** Add user metadata to events

```scala
def denormalize(event: Event, cacheData, metrics): Unit = {
  val actorId = event.actorId()
  val actorType = event.actorType()
  
  if (actorId != null && "User".equalsIgnoreCase(actorType)) {
    metrics.incCounter(config.userTotal)
    
    try {
      val userData = denormCache.getUserData(actorId)
      
      if (userData != null) {
        metrics.incCounter(config.userCacheHit)
        val fields = Map(
          "usertype" → userData.get("usertype"),
          "grade" → userData.get("grade"),
          "language" → userData.get("language"),
          "state" → userData.get("state"),
          "district" → userData.get("district"),
          "subject" → userData.get("subject"),
          "usersignintype" → userData.get("usersignintype"),
          "userlogintype" → userData.get("userlogintype")
        )
        event.addUserData(fields)
      } else {
        metrics.incCounter(config.userCacheMiss)
      }
    } catch {
      case e: Exception ⇒
        metrics.incCounter(config.userCacheMiss)
        throw e
    }
  }
}
```

#### **ContentDenormalization**

**Purpose:** Add content metadata to events

```scala
def denormalize(event: Event, cacheData, metrics): Unit = {
  val objectId = event.objectId()
  val objectType = event.objectType()
  
  if (objectId != null && objectType != null && 
      !"dialcode".equalsIgnoreCase(objectType) && 
      !"qr".equalsIgnoreCase(objectType) && 
      !"user".equalsIgnoreCase(objectType)) {
    
    metrics.incCounter(config.contentTotal)
    
    try {
      // Fetch content data
      val contentData = denormCache.getContentData(objectId)
      
      if (contentData != null) {
        metrics.incCounter(config.contentCacheHit)
        val fields = Map(
          "name" → contentData.get("name"),
          "contentType" → contentData.get("contentType"),
          "framework" → contentData.get("framework"),
          "subject" → contentData.get("subject"),
          "board" → contentData.get("board"),
          "channel" → contentData.get("channel"),
          "status" → contentData.get("status")
        )
        event.addContentData(fields)
      } else {
        metrics.incCounter(config.contentCacheMiss)
      }
      
      // Also fetch collection (parent) data if rollup.l1 exists
      val collectionId = event.getRollupL1()
      if (collectionId != null && !collectionId.equals(objectId)) {
        metrics.incCounter(config.collectionTotal)
        val collectionData = denormCache.getContentData(collectionId)
        if (collectionData != null) {
          metrics.incCounter(config.collectionCacheHit)
          event.addCollectionData(collectionData)
        } else {
          metrics.incCounter(config.collectionCacheMiss)
        }
      }
    } catch {
      case e: Exception ⇒
        logger.error(s"Error fetching content data: ${e.getMessage}", e)
        throw e
    }
  }
}
```

#### **LocationDenormalization**

**Purpose:** Derive location with fallback priority

```scala
def denormalize(event: Event, metrics: Metrics): Unit = {
  var derivedLocation: String = null
  var derivedFrom: String = null
  
  // Priority 1: User profile location (from user cache)
  val userLocation = event.getUserData().map(_.get("state")).flatten
  if (userLocation.isDefined) {
    derivedLocation = userLocation.get
    derivedFrom = "user_profile"
  }
  
  // Priority 2: User declared location (from device cache)
  if (derivedLocation == null) {
    val declaredLocation = event.getDeviceData().map(_.get("user_declared_state")).flatten
    if (declaredLocation.isDefined) {
      derivedLocation = declaredLocation.get
      derivedFrom = "user_declared"
    }
  }
  
  // Priority 3: IP-based location (from device cache)
  if (derivedLocation == null) {
    val ipLocation = event.getDeviceData().map(_.get("state")).flatten
    if (ipLocation.isDefined) {
      derivedLocation = ipLocation.get
      derivedFrom = "ip_location"
    }
  }
  
  // Add to event
  if (derivedLocation != null) {
    event.addLocationData(Map(
      "location" → derivedLocation,
      "derivedFrom" → derivedFrom
    ))
    metrics.incCounter(config.locTotal)
    metrics.incCounter(config.locCacheHit)
  }
}
```

### 3.4 Redis Caches Used

| Cache Name | Data Stored | Host Config | Key Structure |
|-----------|------------|-------------|---------------|
| **Device Cache** | Device specs, geo, device tokens | redis-meta.device | `device_id` |
| **User Cache** | User profile, preferences | redis-meta.user | `user_id` |
| **Content Cache** | Content metadata, hierarchy | redis-meta.content | `content_id` |
| **DialCode Cache** | QR code information | redis-meta.dialcode | `dialcode` |

### 3.5 Configuration

```properties
# Input/Output Topics
kafka.input.telemetry.topic = telemetry.denorm.primary
kafka.input.summary.topic = telemetry.denorm.secondary
kafka.telemetry.denorm.output.topic = telemetry.unique.primary
kafka.summary.denorm.output.topic = telemetry.unique.secondary
kafka.output.failed.topic = telemetry.failed

# Redis Caches
redis-meta.user.host = redis-user-host
redis-meta.device.host = redis-device-host
redis-meta.content.host = redis-content-host
redis-meta.dialcode.host = redis-dialcode-host

redis-meta.user.port = 6379
redis-meta.device.port = 6379
redis-meta.content.port = 6379
redis-meta.dialcode.port = 6379

redis-meta.database.userstore.id = 0
redis-meta.database.devicestore.id = 0
redis-meta.database.contentstore.id = 0
redis-meta.database.dialcodestore.id = 0

# Event Filtering
telemetry.ignore.period.months = 3
summary.filter.events = [ME_WORKFLOW_SUMMARY]
skip.events = [LOG, ERROR]

# Processing
task.consumer.parallelism = 4
task.telemetry.downstream.operators.parallelism = 8
task.summary.downstream.operators.parallelism = 4
```

---

## 4. Druid Events Validator

**Location:** `data-pipeline-flink/druid-events-validator/`

**Role:** Final validation of denormalized events and route to Druid.

### 4.1 Input/Output

| Aspect | Details |
|--------|---------|
| **Input Topic** | `telemetry.unique.primary`, `telemetry.unique.secondary` |
| **Input Format** | Denormalized events (with device, user, content data) |
| **Output Topics** | `druid.events.telemetry`, `telemetry.duplicate`, `telemetry.failed`, Summary/Log/Error routes |
| **Output Format** | Validated events ready for Druid ingestion |

### 4.2 Processing Flow

```
Input Denormalized Event
    │
    ├─► DruidValidatorFunction
    │   │
    │   ├─► Optional: Deduplication Check
    │   │   ├─ If druid.deduplication.enabled = true
    │   │   │   ├─ Extract mid (message ID)
    │   │   │   ├─ Check Redis for mid
    │   │   │   ├─ If exists → Send to duplicate topic, return false
    │   │   │   └─ If not → Add to Redis, continue
    │   │   └─ If disabled → continue without dedup
    │   │
    │   ├─► Optional: Schema Validation
    │   │   ├─ If druid.validation.enabled = true
    │   │   │   ├─ Validate against Druid-specific schema
    │   │   │   ├─ Check denormalized data structure
    │   │   │   ├─ If valid → mark success, continue
    │   │   │   └─ If invalid → mark failure, send to failed topic
    │   │   └─ If disabled → continue
    │   │
    │   └─► Route Events
    │       ├─ If isSummaryEvent → Send to summary router output tag
    │       └─ Else → Send to telemetry router output tag
    │
    ├─► Output Routing
    │   ├─ Summary Events:
    │   │   ├─ druid.events.telemetry (summary-related)
    │   │   └─ Include in Druid analytics
    │   │
    │   ├─ Telemetry Events:
    │   │   ├─ druid.events.telemetry (main)
    │   │   └─ Include in Druid analytics
    │   │
    │   ├─ Invalid Events:
    │   │   └─ telemetry.failed (for debugging)
    │   │
    │   ├─ Duplicate Events:
    │   │   └─ telemetry.duplicate (for tracking)
    │   │
    │   ├─ Log Events:
    │   │   └─ log router topic
    │   │
    │   └─ Error Events:
    │       └─ error router topic
    │
    └─► Sink to Kafka → Druid Ingestion
```

### 4.3 Key Functions

#### **DruidValidatorFunction**

**Purpose:** Final event validation before Druid ingestion

```scala
// Located in: DruidValidatorFunction.scala

def processElement(event: Event, ctx, metrics): Unit = {
  
  // Step 1: Optional Deduplication
  val isUnique =
    if (config.druidDeduplicationEnabled) {
      deDuplicate[Event, Event](event.mid(), event, ctx, 
        config.duplicateEventOutputTag,
        flagName = "dv_duplicate")(dedupEngine, metrics)
    } else true
  
  if (!isUnique) return  // Duplicate, already routed
  
  // Step 2: Optional Schema Validation
  val routeEventsDownstream =
    if (config.druidValidationEnabled) {
      validateEvent(event, ctx, metrics)
    } else true
  
  if (!routeEventsDownstream) return  // Invalid, already routed
  
  // Step 3: Route Events
  routeEvents(event, ctx, metrics)
}

def validateEvent(event: Event, ctx, metrics): Boolean = {
  val validationReport = schemaValidator.validate(event)
  
  if (validationReport.isSuccess) {
    event.markValidationSuccess()
    metrics.incCounter(config.validationSuccessMetricsCount)
    true  // Continue to routing
  } else {
    val failedErrorMsg = schemaValidator.getInvalidFieldName(validationReport.toString)
    event.markValidationFailure(failedErrorMsg)
    metrics.incCounter(config.validationFailureMetricsCount)
    ctx.output(config.invalidEventOutputTag, event)  // Send to failed topic
    false  // Stop processing
  }
}

def routeEvents(event: Event, ctx, metrics): Unit = {
  if (event.isSummaryEvent) {
    // Summary event (ME_WORKFLOW_SUMMARY, etc.)
    metrics.incCounter(config.summaryRouterMetricCount)
    ctx.output(config.summaryRouterOutputTag, event)
  } else {
    // Regular telemetry event
    metrics.incCounter(config.telemetryRouterMetricCount)
    ctx.output(config.telemetryRouterOutputTag, event)
  }
}
```

**Validation Process:**
1. **Deduplication**: Optional, removes duplicate summaries/events
2. **Schema Check**: Ensures all denormalized fields are present and valid
3. **Routing**: Splits events by type for different Druid tables

**Metrics Tracked:**
- `validation-success-message-count` - Valid events
- `validation-failed-message-count` - Failed validation
- `summary-route-success-count` - Summary events routed
- `telemetry-route-success-count` - Telemetry events routed

### 4.4 Output Tags & Kafka Topics

```scala
telemetryRouterOutputTag    → druid.events.telemetry
summaryRouterOutputTag      → druid.events.summary
invalidEventOutputTag       → telemetry.failed
duplicateEventOutputTag     → telemetry.duplicate
```

### 4.5 Configuration

```properties
# Input/Output Topics
kafka.input.topic = telemetry.unique.primary
kafka.output.telemetry.route.topic = druid.events.telemetry
kafka.output.summary.route.topic = druid.events.summary
kafka.output.failed.topic = telemetry.failed
kafka.output.duplicate.topic = telemetry.duplicate

# Validation & Dedup Settings
task.druid.validation.enabled = true
task.druid.deduplication.enabled = true

# Schema Paths (for final validation)
schema.path.telemetry = /schemas/druid/telemetry/
schema.path.summary = /schemas/druid/summary/
schema.file.default = telemetry.json
schema.file.search = search.json
schema.file.summary = summary.json

# Redis for dedup
redis.database.duplicationstore.id = 0
redis.database.key.expiry.seconds = 86400

# Processing
task.consumer.parallelism = 4
task.downstream.operators.parallelism = 8
```

---

## Technology Stack

### Core Technologies

| Technology | Purpose | Version |
|-----------|---------|---------|
| **Apache Flink** | Stream processing framework | 1.10.x - 1.13.x |
| **Scala** | Programming language | 2.11 / 2.12 |
| **Kafka** | Message broker | 2.x |
| **Redis** | Cache & dedup store | 5.x - 6.x |
| **Druid** | Analytics database | 0.20.x+ |

### Libraries & Dependencies

```xml
<!-- Flink Core -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala_2.12</artifactId>
  <version>${flink.version}</version>
</dependency>

<!-- Kafka -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka_2.12</artifactId>
  <version>${flink.version}</version>
</dependency>

<!-- Redis -->
<dependency>
  <groupId>redis.clients</groupId>
  <artifactId>jedis</artifactId>
  <version>3.x</version>
</dependency>

<!-- JSON Processing -->
<dependency>
  <groupId>com.google.code.gson</groupId>
  <artifactId>gson</artifactId>
</dependency>

<!-- Schema Validation -->
<dependency>
  <groupId>com.github.fge</groupId>
  <artifactId>json-schema-validator</artifactId>
</dependency>
```

---

## Data Quality & Resilience

### Error Handling Strategy

1. **Validation Failures**: Events fail schema validation → sent to `telemetry.failed` topic
2. **Deduplication**: Duplicate events → sent to `telemetry.duplicate` topic
3. **Size Violations**: Events > 1 MB → sent to failed topic
4. **Redis Failures**: Connection errors → job stops (fail-fast approach)
5. **Age Filter**: Events > 3 months old → sent to failed topic

### Metrics & Monitoring

Each job publishes metrics to track:
- **Success counts**: Events processed successfully
- **Failure counts**: Validation/processing failures
- **Duplicate counts**: Duplicate events detected
- **Cache hits/misses**: Redis cache performance
- **Event type counts**: Distribution of event types

### Deduplication Windows

| Level | Storage | TTL | Key |
|-------|---------|-----|-----|
| **Batch** (Extractor) | Redis DB 0 | 24 hours | msgid (batch message ID) |
| **Event** (Preprocessor) | Redis DB 0 | 24 hours | mid (event message ID) |
| **Summary** (Validator) | Redis DB 0 | 24 hours | mid (summary message ID) |

### Retention & Archival

- **Valid events**: Stored in Druid indefinitely
- **Failed events**: Retained in `telemetry.failed` for debugging (configurable retention)
- **Duplicates**: Retained in `telemetry.duplicate` for audit trail
- **Archive**: Events also stored in cloud storage via Secor for backup/replay

---

## Common Debugging Tips

### Event Not Appearing in Druid

1. Check `telemetry.failed` topic for validation errors
2. Check `telemetry.duplicate` topic (might be deduplicated)
3. Verify schema is correct for event type (eid)
4. Check Redis connectivity in denorm job logs

### High Duplicate Count

1. Check if same batch/event ID being sent multiple times
2. Verify `redis.database.key.expiry.seconds` is appropriate
3. Check for Kafka replay scenarios

### Redis Cache Misses

1. Check if device/content/user IDs exist in cache
2. Verify correct Redis host/port/database ID in config
3. Check Redis memory limits and eviction policy

### Performance Tuning

1. Increase `task.consumer.parallelism` for throughput
2. Adjust `task.downstream.operators.parallelism` based on CPU
3. Monitor Kafka topic lag
4. Monitor Redis memory usage

---

## Summary Table: Job Responsibilities

| Job | Input | Key Functions | Output | Metrics |
|-----|-------|---------------|--------|---------|
| **Ingest Router** | telemetry.ingestion (ingest) / telemetry.raw.event (raw) | Pass-through byte routing, no transformation | telemetry.ingest (ingest) / telemetry.raw (raw) | ingest-success-event-count, raw-success-event-count |
| **Extractor** | telemetry.ingest | Dedup batch, unpack events, audit generation | telemetry.raw, assess.raw | success/fail/duplicate counts |
| **Preprocessor** | telemetry.raw | Schema validation, data correction, dedup, routing | telemetry.unique, audit, log, error | validation/dedup counts |
| **Denorm** | telemetry.denorm.* | Device/User/Content/Location enrichment | telemetry.unique.* | cache hits/misses, enrichment counts |
| **Validator** | telemetry.unique.* | Final validation, dedup summaries, routing | druid.events.telemetry | validation/routing counts |

---

**End of Architecture Guide**

For questions or updates, contact the Data Pipeline team.
