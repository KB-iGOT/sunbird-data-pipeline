# Sunbird Analytics Data Pipeline — Low Level Design

> Auto-generated from source code analysis. Last updated: 2026-06-17

# Sunbird Analytics Data Pipeline — Low-Level Design

---

## 1. dp-core Framework

### 1.1 BaseJobConfig

**Class:** `BaseJobConfig(config: Config, jobName: String) extends Serializable`
**serialVersionUID:** `1234567`

#### Kafka Consumer Config Keys

| Field | Config Key | Type | Default |
|---|---|---|---|
| `kafkaConsumerBrokerServers` | `kafka.consumer.broker-servers` | String | — |
| `groupId` | `kafka.groupid` | String | — |
| `kafkaConsumerParallelism` | `task.consumer.parallelism` | Int | — |
| `kafkaAutoOffsetReset` | `kafka.consumer.auto.offset.reset` | Option[String] | None |

**`kafkaConsumerProperties()`** builds `java.util.Properties`:
- `bootstrap.servers` = `kafkaConsumerBrokerServers`
- `group.id` = `groupId`
- `isolation.level` = `read_committed`
- `auto.offset.reset` = value if `kafkaAutoOffsetReset` is Some (test-only)

#### Kafka Producer Config Keys

| Field | Config Key | Type | Default |
|---|---|---|---|
| `kafkaProducerBrokerServers` | `kafka.producer.broker-servers` | String | — |
| `kafkaProducerMaxRequestSize` | `kafka.producer.max-request-size` | Int | — |
| `kafkaProducerBatchSize` | `kafka.producer.batch.size` | Int | — |
| `kafkaProducerLingerMs` | `kafka.producer.linger.ms` | Int | — |
| `kafkaProducerCompression` | `kafka.producer.compression` | String | `snappy` |

**`kafkaProducerProperties()`** builds `java.util.Properties`:
- `bootstrap.servers`, `linger.ms`, `batch.size`, `compression.type`, `max.request.size`

#### Redis Config Keys

| Field | Config Key | Purpose |
|---|---|---|
| `redisHost` | `redis.host` | Primary Redis host |
| `redisPort` | `redis.port` | Primary Redis port (default 6379) |
| `redisConnectionTimeout` | `redis.connection.timeout` | Connection timeout ms (default 30000) |
| `metaRedisHost` | `redis-meta.host` | Secondary Redis for content metadata |
| `metaRedisPort` | `redis-meta.port` | Secondary Redis port |

#### Checkpoint Config Keys

| Field | Config Key | Type |
|---|---|---|
| `enableCompressedCheckpointing` | `job.enable.compressed.checkpointing` | Boolean |
| `checkpointingInterval` | `job.checkpointing.interval` | Int (ms) |
| `checkpointingPauseSeconds` | `job.checkpointing.pause.between.seconds` | Int (s) |
| `enableDistributedCheckpointing` | `job.enable.distributed.checkpointing` | Option[Boolean] |
| `checkpointingBaseUrl` | `job.statebackend.base.url` | Option[String] |

#### Job Execution Config

| Field | Config Key | Type |
|---|---|---|
| `restartAttempts` | `task.restart-strategy.attempts` | Int |
| `delayBetweenAttempts` | `task.restart-strategy.delay` | Long (ms) |
| `parallelism` | `task.parallelism` | Int |

**Implicit:** `metricTypeInfo: TypeInformation[String]` — used for `OutputTag` type declarations.

---

### 1.2 BaseProcessFunction

#### Case Class `Metrics`

Backed by `ConcurrentHashMap[String, AtomicLong]`.

| Method | Signature | Behaviour |
|---|---|---|
| `incCounter` | `(metric: String): Unit` | `AtomicLong.incrementAndGet()` |
| `getAndReset` | `(metric: String): Long` | `getAndSet(0)` — read then zero |
| `get` | `(metric: String): Long` | `AtomicLong.get()` |
| `reset` | `(metric: String): Unit` | Sets to 0 |

#### Trait `JobMetrics`

`registerMetrics(metrics: List[String]): Metrics` — constructs a `ConcurrentHashMap` with one `AtomicLong(0)` per name.

#### Abstract Class `BaseProcessFunction[T, R]`

Extends `ProcessFunction[T, R]`, `BaseDeduplication`, `JobMetrics`. Constructor takes `config: BaseJobConfig`.

**Lifecycle:**

1. **`open(parameters: Configuration)`** — calls `super.open()`, then for each name in `metricsList()` registers a Flink `Gauge` under `getRuntimeContext.getMetricGroup.addGroup(config.jobName)`. Each gauge calls `metrics.getAndReset(name)` so the value is both reported and cleared each interval.
2. **`processElement(event: T, context: Context, out: Collector[R])`** — delegates to the abstract `processElement(event, context, metrics)`.
3. **`close()`** — inherited default (subclasses override for resource cleanup).

**Abstract methods subclasses must implement:**
- `processElement(event: T, context: Context, metrics: Metrics): Unit`
- `metricsList(): List[String]`

#### Abstract Class `WindowBaseProcessFunction[I, O, K]`

Extends `ProcessWindowFunction[I, O, K, GlobalWindow]`, `BaseDeduplication`, `JobMetrics`. Follows identical gauge-registration lifecycle in `open()`. Delegates to:
- `process(key: K, context: Context, elements: Iterable[I], metrics: Metrics): Unit`

---

### 1.3 BaseDeduplication

**Trait** mixed into `BaseProcessFunction` and `WindowBaseProcessFunction`.

#### Constants

| Constant | Value | Purpose |
|---|---|---|
| `uniqueEventMetricCount` | `"unique-event-count"` | Counter for events passing dedup |
| `duplicateEventMetricCount` | `"duplicate-event-count"` | Counter for rejected duplicates |

#### Methods

**`deDup[T, R](key, event, context, successOutputTag, duplicateOutputTag, flagName)(implicit deDupEngine, metrics)`**

Algorithm:
1. If `key == null` OR `deDupEngine.isUniqueEvent(key) == false`: call `updateFlag(event, flagName, true)`, emit to `duplicateOutputTag`, increment `duplicateEventMetricCount`.
2. Else: call `deDupEngine.storeChecksum(key)`, call `updateFlag(event, flagName, false)`, emit to `successOutputTag`, increment `uniqueEventMetricCount`.

**`deDuplicate[T, R](key, event, context, duplicateOutputTag, flagName)(implicit deDupEngine, metrics): Boolean`**

Variant: duplicates go to `duplicateOutputTag` with flag `true`. Unique events are NOT emitted here — the caller handles them. Returns `true` if unique.

**`updateFlag[T, R](event: T, flagName: String, value: Boolean): R`**

Handles three event shapes in order:
1. `Events` subclass — calls `event.updateFlags(flagName, value)`
2. `String` — deserializes JSON to `Map`, sets `flags[flagName]`, re-serializes
3. `Map[String, AnyRef]` — mutates `flags` sub-map in place

**`deduplicationMetrics: List[String]`** — returns `List("unique-event-count", "duplicate-event-count")`.

#### Redis Key Pattern

The key passed to `DedupEngine` is the event's `msgid` (extractor) or `mid()` (preprocessor). The engine stores it as a plain Redis string key with `SETEX` and the configured TTL. There is no namespace prefix — the DB index provides isolation.

---

### 1.4 FlinkKafkaConnector

**Class:** `FlinkKafkaConnector(config: BaseJobConfig) extends Serializable`

All sinks use `Semantic.AT_LEAST_ONCE`. Serialization/deserialization is handled by schema classes in `org.sunbird.dp.core.serde`.

| Method | Source/Sink | Schema Class | Type |
|---|---|---|---|
| `kafkaStringSource(topic)` | Source | `StringDeserializationSchema` | `String` |
| `kafkaBytesSource(topic)` | Source | `ByteDeserializationSchema` | `Array[Byte]` |
| `kafkaMapSource(topic)` | Source | `MapDeserializationSchema` | `util.Map[String,AnyRef]` |
| `kafkaEventSource[T <: Events](topic)` | Source | `EventDeserializationSchema[T]` | `T` |
| `kafkaStringSink(topic)` | Sink | `StringSerializationSchema` | `String` |
| `kafkaBytesSink(topic)` | Sink | `ByteSerializationSchema` | `Array[Byte]` |
| `kafkaMapSink(topic)` | Sink | `MapSerializationSchema` | `util.Map[String,AnyRef]` |
| `kafkaEventSink[T <: Events](topic)` | Sink | `EventSerializationSchema[T]` | `T` |

`kafkaEventSource`/`kafkaEventSink` require an implicit `Manifest[T]` for runtime reflection.

---

### 1.5 RedisConnect

**Class:** `RedisConnect(redisHost: String, redisPort: Int, config: BaseJobConfig) extends Serializable`

No connection pooling. Every `getConnection` call creates a fresh `Jedis` socket.

| Method | Signature | Behaviour |
|---|---|---|
| `getConnection(db, backoffMs)` | private | Optional `Thread.sleep(backoffMs)`, then `new Jedis(host, port, timeout)`, selects `db` |
| `getConnection(db)` | public | Calls above with `backoffMs=0` |
| `getConnection()` | public | Calls `getConnection(db=0)` |

`backoffMs=10000` is used by `DedupEngine` on reconnect attempts to prevent thundering herd.

---

### 1.6 DataCache

**Class:** `DataCache(config: BaseJobConfig, redisConnect: RedisConnect, dbIndex: Int, fields: List[String])`

**Lifecycle:** `init()` calls `redisConnect.getConnection(dbIndex)` and stores the `Jedis` handle. `close()` closes it.

#### Read Operations

| Method | Redis Command | Retry | Notes |
|---|---|---|---|
| `hgetAllWithRetry(key)` | `HGETALL` | Yes (reconnect once) | Filters to declared `fields`, strips empty strings, converts JSON strings via `convertToComplexDataTypes` |
| `getWithRetry(key)` | `GET` | Yes (reconnect once) | Parses JSON to `HashMap`, applies field projection |
| `getMultipleWithRetry(keys)` | `GET` per key | Via `getWithRetry` | Loops over key list |
| `isExists(key)` | `EXISTS` | No | Raw check |
| `sMembers(key)` | `SMEMBERS` | No | Returns Redis set |
| `getKeyMembers(key)` | `SMEMBERS` | Yes (reconnect) | Retry variant |

**`convertToComplexDataTypes(value: String)`** — if value starts with `[` deserializes as `ArrayList`; if `{` as `HashMap`; otherwise raw `String`.

#### Write Operations

| Method | Redis Command | Retry |
|---|---|---|
| `hmSet(key, map)` | `HMSET` | Yes |
| `setWithRetry(key, value)` / `set(key, value)` | `SET` | Yes |
| `hdelWithRetry(key, fields)` | `HDEL` | Yes |
| `hIncByWithRetry(key, field, count)` | `HINCRBY` | Yes |

---

### 1.7 DedupEngine

**Class:** `DedupEngine(redisConnect: RedisConnect, store: Int, expirySeconds: Int)`

Immediately on construction: `jedis = redisConnect.getConnection(store)`.

| Method | Redis Command | On `JedisException` |
|---|---|---|
| `isUniqueEvent(checksum)` | `EXISTS checksum` | Close, sleep 10000ms, reconnect, retry |
| `storeChecksum(checksum)` | `SETEX checksum expirySeconds "1"` | Close, sleep 10000ms, reconnect, re-select DB, retry |
| `getRedisConnection()` | — | Returns raw `Jedis` (test use) |
| `closeConnectionPool()` | — | `jedis.close()` |

The two methods are intentionally separate to allow the caller (`BaseDeduplication.deDup`) to control the sequence: check uniqueness first, then conditionally store.

---

### 1.8 Events Domain Model

**Abstract class:** `Events(val map: util.Map[String, Any])`

Internal state: `telemetry: Telemetry` — wraps `map` for dot-notation path access.

#### Key Field Accessors (via `EventsPath` constants)

| Method | Path / Logic | EventsPath Constant |
|---|---|---|
| `mid()` | `"mid"` | `MID_PATH` |
| `eid()` | `"eid"` | `EID_PATH` |
| `ets()` | `"ets"` as Long | `ETS_PATH` |
| `id()` | `"metadata.checksum"` | `CHECKSUM_PATH` |
| `getChecksum` | `id()` if non-null else `mid()` | — |
| `did()` | `"dimensions.did"` fallback `"context.did"` | `DIMENSION_DID_PATH`, `CONTEXT_DID_PATH` |
| `channel()` | `"dimensions.channel"` fallback `"context.channel"` | `DIMENSION_CHANNEL_PATH`, `CONTEXT_CHANNEL_PATH` |
| `actorId()` | `"uid"` fallback `"actor.id"` | `UID_PATH`, `ACTOR_ID_PATH` |
| `actorType()` | `"actor.type"` | `ACTOR_TYPE_PATH` |
| `pid()` | `"context.pdata.pid"` | `CONTEXT_P_DATA_PID_PATH` |
| `producerId()` | `"context.pdata.id"` | `CONTEXT_P_DATA_ID_PATH` |
| `objectID()` | `"object.id"` (if both id+type non-null) | `OBJECT_ID_PATH` |
| `objectType()` | `"object.type"` (if both id+type non-null) | `OBJECT_TYPEPATH` |
| `edataType()` | `"edata.type"` | `EDATA_TYPE_PATH` |
| `edataItems()` | `"edata.items"` as `List[Map[String,AnyRef]]` | `EDATA_ITEM` |
| `flags()` | `"flags"` map | `FLAGS_PATH` |
| `version()` | `"ver"` | `VERSION_KEY_PATH` |

#### Mutation Methods

| Method | Effect |
|---|---|
| `updateFlags(key, value)` | Creates `flags` map if absent; sets `flags[key] = value` |
| `updateTs(value)` | Writes `"@timestamp"` field |
| `kafkaKey()` | Returns `mid()` — Kafka partition key |
| `getJson()` | Serializes `map` via `JSONUtil.serialize` |

#### Telemetry Path Navigation

`Telemetry.read[T](path)` splits on `"."` and traverses nested `util.Map` instances. Returns `None` if any intermediate key is absent. `add(path, value)` navigates to the parent map and writes at the leaf key.

---

### 1.9 Supporting Utilities

#### JSONUtil

Singleton Jackson `ObjectMapper` (`@transient` — not serialized by Flink).

Config: `FAIL_ON_UNKNOWN_PROPERTIES=false`, `FAIL_ON_EMPTY_BEANS=false`, `WRITE_BIGDECIMAL_AS_PLAIN=true`, `NON_NULL` inclusion.

| Method | Input | Output |
|---|---|---|
| `serialize(obj)` | `AnyRef` | JSON `String` |
| `deserialize[T](s: String)` | `String` + `Manifest[T]` | `T` |
| `deserialize[T](bytes: Array[Byte])` | `Array[Byte]` + `Manifest[T]` | `T` (avoids intermediate String) |

#### FlinkUtil

`getExecutionContext(config)` constructs `StreamExecutionEnvironment`:
1. Optional snapshot compression if `enableCompressedCheckpointing=true`
2. `enableCheckpointing(checkpointingInterval)`
3. If `enableDistributedCheckpointing=true`: `FsStateBackend(checkpointingBaseUrl/jobName)`, `ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION`, `setMinPauseBetweenCheckpoints(checkpointingPauseSeconds * 1000)`
4. `setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)`
5. `setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts))`

#### EventSerde

**`EventDeserializationSchema[T <: Events]`** — `isEndOfStream` always `false`. On parse failure: logs exception, returns empty-map instance of `T` (fail-soft, no crash). Uses `ClassTag[T]` for reflective constructor invocation.

**`EventSerializationSchema[T <: Events]`** — key = `element.kafkaKey()` (null if absent); value = `element.getJson().getBytes(UTF_8)`.

#### MapSerde

**Asymmetry:** `MapDeserializationSchema` uses Jackson (`JSONUtil`); `MapSerializationSchema` uses Gson. The optional `key` parameter in `MapSerializationSchema` sets the Kafka partition key.

#### CassandraUtil

Single contact point. Consistency: `QUORUM` globally. `findOne` returns first row or null. `find` catches `DriverException`, logs query, rethrows (auto-reconnect disabled). `reconnect()` must be called manually.

#### PostgresConnect

Uses deprecated `PGPoolingDataSource`. `execute()` auto-retries once via `resetConnection()` on `SQLException`. `executeQuery()` has no retry.

#### RestUtil

No connection pooling — new `HttpClient` per `GET` call. No retry. Returns raw response body as `String`.

---

## 2. TelemetryExtractor (Flink Job)

### 2.1 Stream Task Topology

```
kafkaInputTopic (String)
        |
        v
[DeduplicationFunction] (parallelism: kafkaConsumerParallelism)
        |-- uniqueEventOutputTag      --> [ExtractionFunction]
        |-- duplicateEventOutputTag   --> kafkaDuplicateTopic
        `-- failedBatchEventOutputTag --> kafkaBatchFailedTopic

[ExtractionFunction] (parallelism: downstreamOperatorsParallelism)
        |-- rawEventsOutputTag          --> kafkaSuccessTopic
        |-- logEventsOutputTag          --> kafkaLogRouteTopic
        |-- auditEventsOutputTag        --> kafkaLogRouteTopic
        |-- assessRedactEventsOutputTag --> [RedactorFunction]
        `-- failedEventsOutputTag       --> kafkaFailedTopic

[RedactorFunction] (parallelism: downstreamOperatorsParallelism)
        |-- rawEventsOutputTag     --> kafkaSuccessTopic
        `-- assessRawEventsOutputTag --> kafkaAssessRawTopic
```

**Entry point:** `TelemetryExtractorStreamTask.main()` — parses `--config.file.path` arg, falls back to `telemetry-extractor.conf`.

---

### 2.2 DeduplicationFunction

**Class:** `DeduplicationFunction extends BaseProcessFunction[String, util.Map[String,AnyRef]]`

**Input:** Raw string batch event JSON from `kafkaInputTopic`.

**Redis:** DB index = `config.dedupStore` (`redis.database.duplicationstore.id`), TTL = `config.cacheExpirySeconds` (`redis.database.key.expiry.seconds`). Connection opened in `open()`, closed in `close()`.

**Dedup Key:** Extracted via `getMsgIdentifier(batchEvents: String): String` — deserializes the batch JSON to `LinkedHashMap`, returns `params.msgid`. Returns `null` if absent (treated as duplicate by `deDup`).

**`processElement` logic:**
1. Call `deDup(getMsgIdentifier(batchEvents), batchEvents, context, uniqueEventOutputTag, duplicateEventOutputTag, "extractor_duplicate")`
2. Increment `successBatchCount`
3. On `JedisException`: log, close Redis, rethrow (Flink restart strategy triggers)
4. On any other `Exception`: log, increment `failedBatchCount`, output raw string to `failedBatchEventOutputTag`

**Metrics:** `success-batch-count`, `failed-batch-count`, `unique-event-count`, `duplicate-event-count`

**Output tags:**

| Tag | Destination |
|---|---|
| `uniqueEventOutputTag` | `ExtractionFunction` |
| `duplicateEventOutputTag` | `kafkaDuplicateTopic` |
| `failedBatchEventOutputTag` | `kafkaBatchFailedTopic` |

---

### 2.3 ExtractionFunction

**Class:** `ExtractionFunction extends BaseProcessFunction[util.Map[String,AnyRef], util.Map[String,AnyRef]]`

**Input format (batch envelope):**

```json
{
  "id": "...",
  "params": { "msgid": "...", "ver": "...", "sync_status": "..." },
  "events": [ { "eid": "...", "mid": "...", ... }, ... ],
  "context": { "channel": "...", "env": "...", "did": "...", "pdata": {...} }
}
```

**`processElement` loop:**
1. `getEventsList(batchEvent)` — returns `batchEvent.get("events")` as `ArrayList`, or empty list
2. Get `syncts`: from `params.syncts` or `System.currentTimeMillis()`
3. For each event:
   - `updateEvent(event, syncts)` — formats `syncts` as `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` UTC, sets `syncts` and `@timestamp` fields
   - Serialize to JSON, check byte size against `config.eventMaxSize`
   - If oversized: `markFailed(event)` → emit to `failedEventsOutputTag`, increment `failedEventCount`
   - Else if `eid` in `config.redactEventsList`: emit to `assessRedactEventsOutputTag`
   - Else if `eid == "LOG"`: emit to `logEventsOutputTag`
   - Else: `markSuccess(event)`, emit to `rawEventsOutputTag`, increment `successEventCount`
4. After loop: `generateAuditEvents(totalEvents, batchEvent)` → emit `LogEvent` to `auditEventsOutputTag`, increment `auditEventCount`

**`markFailed(event)`** sets: `flags.ex_processed=false`, `metadata.src=jobName`, `metadata.ex_error="Event size is Exceeded"`

**`markSuccess(event)`** sets: `flags.ex_processed=true`

**`generateAuditEvents`** produces a `LogEvent` case class:
- `eid="LOG"`, `actor=(sunbird.telemetry, telemetry-sync)`, `edata.params=[{events_count=totalEvents, sync_status=SUCCESS}]`
- `context` sourced from `batchEvent` fields: `channel`, `env`, `did`, `pdata`
- `mid` from batch `params.msgid`

**Config params:**

| Config Key | Field | Purpose |
|---|---|---|
| `kafka.event.max.size` | `eventMaxSize: Long` | Max bytes per individual event |
| `redact.events.list` | `redactEventsList: List[String]` | EIDs routed to redaction |

**Metrics:** `success-event-count`, `failed-event-count`, `audit-event-count`

---

### 2.4 RedactorFunction

**Class:** `RedactorFunction extends BaseProcessFunction[util.Map[String,AnyRef], util.Map[String,AnyRef]]`

**Input:** `assessRedactEventsOutputTag` — events with `eid` matching `redactEventsList` (typically `ASSESS`, `RESPONSE`).

**Redis (metadata):** Uses `metaRedisHost`/`metaRedisPort` (secondary Redis). DB index = `config.contentStore` (`redis-meta.database.contentstore.id`). `DataCache` initialized with `fields=List("questionType")`. Connection managed via `dataCache.init()` / `dataCache.close()`.

**`processElement` logic:**
1. `getQuestionId(event)`:
   - If `eid == "ASSESS"`: return `edata.item.id`
   - Else: return `edata.target.id`
2. If `questionId == null`: increment `skippedEventCount`, skip redaction
3. `getQuestionData(questionId)` — calls `dataCache.getWithRetry(questionId)`
4. If result is empty: increment `cacheMissCount`
5. If non-empty: increment `cacheHitCount`; if `questionType.equalsIgnoreCase("Registration")`: set `redact=true`
6. If `redact==true`:
   - Emit original (pre-redaction) event to `assessRawEventsOutputTag`
   - `clearUserInputData(event)`:
     - If `eid == "ASSESS"`: set `edata.resvalues = []`
     - Else: set `edata.values = []`
7. Always emit event (possibly redacted) to `rawEventsOutputTag`

**Trigger condition for raw copy:** `questionType == "Registration"` (case-insensitive).

**Fields redacted:** `edata.resvalues` (ASSESS events), `edata.values` (RESPONSE events) — both set to empty `ArrayList`.

**Metrics:** `skipped-event-count`, `cache-miss-count`, `cache-hit-count`

---

### 2.5 TelemetryExtractorConfig Reference

| Config Key | Field | Type |
|---|---|---|
| `redis.database.duplicationstore.id` | `dedupStore` | Int |
| `redis.database.key.expiry.seconds` | `cacheExpirySeconds` | Int |
| `redis-meta.database.contentstore.id` | `contentStore` | Int |
| `kafka.input.topic` | `kafkaInputTopic` | String |
| `kafka.output.success.topic` | `kafkaSuccessTopic` | String |
| `kafka.output.log.route.topic` | `kafkaLogRouteTopic` | String |
| `kafka.output.duplicate.topic` | `kafkaDuplicateTopic` | String |
| `kafka.output.failed.topic` | `kafkaFailedTopic` | String |
| `kafka.output.batch.failed.topic` | `kafkaBatchFailedTopic` | String |
| `kafka.output.assess.raw.topic` | `kafkaAssessRawTopic` | String |
| `task.consumer.parallelism` | `kafkaConsumerParallelism` | Int |
| `task.downstream.operators.parallelism` | `downstreamOperatorsParallelism` | Int |
| `kafka.event.max.size` | `eventMaxSize` | Long |
| `redact.events.list` | `redactEventsList` | List[String] |

### 2.6 Input/Output Topics Summary

| Direction | Topic Field | Usage |
|---|---|---|
| Input | `kafkaInputTopic` | Raw batch events (String JSON) |
| Output | `kafkaSuccessTopic` | Extracted individual events + redacted assess events |
| Output | `kafkaLogRouteTopic` | LOG events + audit LOG events |
| Output | `kafkaDuplicateTopic` | Duplicate batch events |
| Output | `kafkaFailedTopic` | Oversized individual events |
| Output | `kafkaBatchFailedTopic` | Unparseable / exception batches |
| Output | `kafkaAssessRawTopic` | Pre-redaction copy of Registration question events |

---

## 3. PipelinePreprocessor (Flink Job)

### 3.1 Stream Task Topology

```
kafkaInputTopic (Event)
        |
        v (rebalance)
[PipelinePreprocessorFunction] (single combined function)
        |
        |-- validationFailedEventsOutputTag  --> kafkaFailedTopic
        |-- logEventsOutputTag               --> kafkaLogRouteTopic
        |-- duplicateEventsOutputTag         --> kafkaDuplicateTopic
        |-- errorEventOutputTag              --> kafkaErrorRouteTopic
        |-- denormSecondaryEventsRouteOutputTag --> kafkaDenormSecondaryRouteTopic
        |-- denormPrimaryEventsRouteOutputTag   --> kafkaDenormPrimaryRouteTopic
        |-- auditRouteEventsOutputTag        --> kafkaAuditRouteTopic
        |                                    --> kafkaPrimaryRouteTopic (second sink)
        |-- cbAuditRouteEventsOutputTag      --> kafkaCbAuditRouteTopic
        |-- primaryRouteEventsOutputTag      --> kafkaPrimaryRouteTopic
        `-- shareItemEventOutputTag          --> kafkaPrimaryRouteTopic
```

**Entry point:** `PipelinePreprocessorStreamTask.main()` — falls back to `pipeline-preprocessor.conf`.

**Note:** `TelemetryValidationFunction`, `ShareEventsFlattenerFunction`, and `TelemetryRouterFunction` exist as separate classes but are NOT wired into the active topology. All logic is consolidated in `PipelinePreprocessorFunction`.

---

### 3.2 PipelinePreprocessorFunction

**Class:** `PipelinePreprocessorFunction extends BaseProcessFunction[Event, Event]`

**Transient dependencies** (initialized in `open()`, null-checked for testability):
- `TelemetryValidator`
- `ShareEventsFlattener`
- `DedupEngine` — Redis DB `config.dedupStore`, TTL `config.cacheExpirySeconds`, key = `event.mid()`

**`processElement` step-by-step:**

1. **Validation:** `telemetryValidator.validate(event, context, metrics)` — invalid events are emitted to `validationFailedEventsOutputTag` inside the validator. If invalid, return early.
2. **LOG routing:** If `event.eid() == "LOG"`: emit to `logEventsOutputTag`, return.
3. **Deduplication:** `deDuplicate(event.mid(), event, context, duplicateEventsOutputTag, config.DEDUP_FLAG_NAME)`. Duplicates go to `duplicateEventsOutputTag`. If duplicate, return.
4. **Hub field enrichment:** `addHubField(event)` — for `IMPRESSION`/`INTERACT`/`START`/`END` events only.

**`addHubField` mapping:**

| `event.env` value | Hub value assigned |
|---|---|
| `learn` | `learn` |
| `course` | `learn` |
| `discuss` | `discuss` |
| `network` | `network` |
| `careers` | `careers` |
| `competency` | `competency` |
| `events` | `events` |
| (else) | `other` |

Calls `event.updateHub(hub)`.

5. **Secondary event routing:** if `event.eid()` is in `config.secondaryEvents`: emit to `denormSecondaryEventsRouteOutputTag`.
6. **ERROR special case:** if `eid == "ERROR"`: emit to `errorEventOutputTag` AND `denormPrimaryEventsRouteOutputTag`.
7. **Primary denorm:** all other unique non-LOG events go to `denormPrimaryEventsRouteOutputTag`.
8. **EID-based routing (match):**
   - `AUDIT` → `auditRouteEventsOutputTag`, increment `auditRouteSuccessCount`, increment `primaryRouterMetricCount`
   - `SHARE` → `shareEventsFlattener.flatten(event, context, metrics)` (emits to `primaryRouteEventsOutputTag` and `shareItemEventOutputTag`)
   - `ERROR` → `errorEventOutputTag`
   - `CB_AUDIT` → `cbAuditRouteEventsOutputTag`, increment `cbAuditRouteSuccessCount`
   - default → `primaryRouteEventsOutputTag`, increment `primaryRouterMetricCount`

**Flags set:**

| Flag constant | Value | Meaning |
|---|---|---|
| `VALIDATION_FLAG_NAME` (`pp_validation_processed`) | `true`/`false` | Validation outcome |
| `DEDUP_FLAG_NAME` (`pp_duplicate`) | `true`/`false` | Dedup result |
| `DEDUP_SKIP_FLAG_NAME` (`pp_duplicate_skipped`) | `true` | Dedup not performed |
| `SHARE_EVENTS_FLATTEN_FLAG_NAME` (`pp_share_event_processed`) | `true` | SHARE flattening complete |

---

### 3.3 TelemetryValidationFunction (Reference Class)

**Class:** `TelemetryValidationFunction extends BaseProcessFunction[Event, Event]`

Not wired into active topology. Documents the validation contract used by `TelemetryValidator`.

**`processElement` logic:**
1. Check `schemaFileExists(event.eid())` — if missing schema: `onMissingSchema` → `validationFailedEventsOutputTag`
2. `schemaValidator.validate(event)` — on success: `onValidationSuccess`; on failure: `onValidationFailure`

**`dataCorrection(event: Event): Event`** corrections applied before routing valid events:
- Strip `"f:"` prefix from actor IDs (federated user accounts)
- For `SEARCH` events: rename `dialCodes` key to `dialcodes`
- For `DialCode`/`qr` object types: uppercase `object.id`

**`isDuplicateCheckRequired(producerId)`** — returns `config.includedProducersForDedup.contains(producerId)` (config-driven list, unlike the production function which hardcodes `true`).

**`onValidationSuccess`** — calls `dataCorrection`, marks flag, calls `event.updateDefaults(config)`, then dedup. Unique → `uniqueEventsOutputTag`. Duplicate → `duplicateEventsOutputTag`. Dedup-skipped → `uniqueEventsOutputTag` with `DEDUP_SKIP_FLAG_NAME=true`.

**`onValidationFailure`** — extracts field name from `ProcessingReport`, sets `flags.pp_validation_processed=false`, sets `metadata.validation_err={field_name}`, emits to `validationFailedEventsOutputTag`.

---

### 3.4 SchemaValidator

**Class:** `SchemaValidator extends java.io.Serializable`

**`readResourceFiles(schemaUrl: String): Map[String, JsonSchema]`** — loads JSON Schema files from classpath resources. The base schema path is `config.schemaPath` (`telemetry.schema.path`), default schema file is `"envelope.json"`.

Validation uses the `json-schema-validator` (fge/java library) `ProcessingReport`. The report is passed to `onValidationFailure` which extracts the first failed keyword's pointer path to identify the invalid field.

---

### 3.5 ShareEventsFlattenerFunction (Reference Class)

**Class:** `ShareEventsFlattenerFunction extends BaseProcessFunction[Event, Event]`

Not wired into active topology. Documents the flattening contract used by `ShareEventsFlattener`.

**`processElement` per SHARE event:**
1. `event.edataItems()` — each item has: `id`, `type`, `ver`, `params: List[Map]`
2. If `params` present: for each param compute:
   - `transfers = 0` → `edataType = "download"`
   - `transfers > 0` → `edataType = "import"`
   - Extract `size` from param
   - Call `generateShareItemEvents(event, eventObj, Some(edataType), Some(paramSize))`
   - Emit `SHARE_ITEM` event to `shareItemEventOutputTag`
   - Increment `shareItemEventsMetircsCount`
3. If no `params`: `generateShareItemEvents` with `event.edataType()`
4. Mark SHARE event with `SHARE_EVENTS_FLATTEN_FLAG_NAME=true`
5. Emit SHARE event to `primaryRouteEventsOutputTag`

**`generateShareItemEvents` output structure:**

```json
{
  "eid": "SHARE_ITEM",
  "ver": "3.0",
  "mid": "<original_mid>_<UUID>",
  "actor": { "id": "...", "type": "..." },
  "edata": { "dir": "<dir>", "type": "<type>", "size": <size> },
  "context": { "channel": "...", "env": "...", "sid": "...", "did": "...", "pdata": {...}, "cdata": [...], "rollup": {...} },
  "object": { "id": "<item.id>", "type": "<item.type>", "ver": "<item.ver>", "rollup": { "l1": "<objectID>" } },
  "tags": [...]
}
```

Serialized via Gson.

---

### 3.6 TelemetryRouterFunction (Reference Class)

**Class:** `TelemetryRouterFunction extends BaseProcessFunction[Event, Event]`

Not wired into active topology. Routing logic is reproduced inline in `PipelinePreprocessorFunction`.

**Routing table:**

| `event.eid().toUpperCase()` | Output tag | Metric |
|---|---|---|
| `AUDIT` | `auditRouteEventsOutputTag` | `auditEventRouterMetricCount`, `primaryRouterMetricCount` |
| `SHARE` | `shareRouteEventsOutputTag` | `shareEventsRouterMetricCount`, `primaryRouterMetricCount` |
| `LOG` | `logEventsOutputTag` | `logEventsRouterMetricsCount` |
| `ERROR` | `errorEventOutputTag` | `errorEventsRouterMetricsCount` |
| default | `primaryRouteEventsOutputTag` | `primaryRouterMetricCount` |

---

### 3.7 PipelinePreprocessorConfig Reference

| Config Key | Field | Type |
|---|---|---|
| `telemetry.schema.path` | `schemaPath` | String |
| `redis.database.duplicationstore.id` | `dedupStore` | Int |
| `redis.database.key.expiry.seconds` | `cacheExpirySeconds` | Int |
| `kafka.input.topic` | `kafkaInputTopic` | String |
| `kafka.output.primary.route.topic` | `kafkaPrimaryRouteTopic` | String |
| `kafka.output.log.route.topic` | `kafkaLogRouteTopic` | String |
| `kafka.output.error.route.topic` | `kafkaErrorRouteTopic` | String |
| `kafka.output.audit.route.topic` | `kafkaAuditRouteTopic` | String |
| `kafka.output.cb.audit.route.topic` | `kafkaCbAuditRouteTopic` | String |
| `kafka.output.failed.topic` | `kafkaFailedTopic` | String |
| `kafka.output.duplicate.topic` | `kafkaDuplicateTopic` | String |
| `kafka.output.denorm.secondary.route.topic` | `kafkaDenormSecondaryRouteTopic` | String |
| `kafka.output.denorm.primary.route.topic` | `kafkaDenormPrimaryRouteTopic` | String |
| `dedup.producer.included.ids` | `includedProducersForDedup` | List[String] |
| `secondary.events` | `secondaryEvents` | List[String] |
| `task.consumer.parallelism` | `kafkaConsumerParallelism` | Int |
| `task.downstream.operators.parallelism` | `downstreamOperatorsParallelism` | Int |

---

## 4. De-normalization (Flink Job)

### 4.1 DenormalizationStreamTask Topology

The denormalization job enriches telemetry events with reference data from Redis caches. There are two stream tasks:

**Primary path (DenormalizationStreamTask):**

```
kafkaDenormPrimaryRouteTopic OR kafkaDenormSecondaryRouteTopic (Event)
        |
        v
[DenormalizationFunction]
        |-- main output (enriched events) --> kafkaDenormSuccessTopic
        `-- failedEventsOutputTag         --> kafkaDenormFailedTopic
```

**Summary path (SummaryDenormalizationStreamTask):**

```
kafkaSummaryRouteTopic (Event)
        |
        v
[SummaryDenormalizationFunction]
        |-- main output --> kafkaSummaryDenormSuccessTopic
        `-- failedEventsOutputTag --> kafkaDenormFailedTopic
```

---

### 4.2 DenormalizationFunction

**Class:** `DenormalizationFunction extends BaseProcessFunction[Event, Event]`

**Processing order (sequential enrichment):**

1. **Device denorm** — lookup by `event.did()`
2. **User denorm** — lookup by `event.actorId()`
3. **Content denorm** — lookup by `event.objectID()` when `event.objectType()` matches content types
4. **Dialcode denorm** — lookup by `event.objectID()` when `event.objectType() == "DialCode"`
5. **Location denorm** — derived from device or user location data

On any enrichment lookup failure, the event still proceeds (partial enrichment). Failed events (exception during processing) go to `failedEventsOutputTag`.

---

### 4.3 Denorm Types — Redis DB, Key Patterns, Fields Added

#### User Denorm

| Property | Value |
|---|---|
| Redis instance | Primary (`redisHost`/`redisPort`) |
| Redis DB | `redis.database.userstore.id` |
| Key | `event.actorId()` (uid or actor.id) |
| Redis operation | `HGETALL <userId>` |
| Data written to event | `event.userdata = { usersignintype, userlogintype, framework: { board, grade, subject, medium, id } }` |
| EventsPath constant | `USERDATA_PATH = "userdata"` |

#### Content Denorm

| Property | Value |
|---|---|
| Redis instance | Secondary (`metaRedisHost`/`metaRedisPort`) |
| Redis DB | `redis-meta.database.contentstore.id` |
| Key | `event.objectID()` |
| Condition | `event.objectType()` is one of: `Content`, `TextBook`, `Collection`, `Course`, `TextBookUnit`, etc. |
| Redis operation | `HGETALL <contentId>` |
| Data written to event | `event.contentdata = { name, objectType, contentType, mediaType, language, medium, gradeLevel, subject, board, ... }` |
| EventsPath constant | `CONTENT_DATA_PATH = "contentdata"` |

#### Dialcode Denorm

| Property | Value |
|---|---|
| Redis instance | Secondary (`metaRedisHost`/`metaRedisPort`) |
| Redis DB | `redis-meta.database.dialcodestore.id` |
| Key | `event.objectID()` (uppercased) |
| Condition | `event.objectType() == "DialCode"` |
| Redis operation | `HGETALL <dialcode>` |
| Data written to event | `event.dialcodedata = { identifier, channel, batchCode, ... }` |
| EventsPath constant | `DIAL_CODE_PATH = "dialcodedata"` |

#### Device Denorm

| Property | Value |
|---|---|
| Redis instance | Primary (`redisHost`/`redisPort`) |
| Redis DB | `redis.database.devicestore.id` |
| Key | `event.did()` |
| Redis operation | `HGETALL <deviceId>` |
| Data written to event | `event.devicedata = { devicespec, os, make, location, stateName, districtName, ... }` |
| EventsPath constant | `DEVICE_DATA_PATH = "devicedata"` |

#### Location Denorm (Derived)

| Property | Value |
|---|---|
| Source | `devicedata.state` / `devicedata.district` or user profile state/district |
| Redis DB | `redis.database.locationstore.id` |
| Key | state name or district name |
| Data written to event | `event.derivedlocationdata = { state, district, from }` |
| `from` field values | `"device"` or `"user"` (indicates data source) |
| EventsPath constants | `DERIVED_LOCATION_PATH = "derivedlocationdata"`, `LOCATION_DERIVED_FROM_PATH = "from"` |

---

### 4.4 DenormCache vs DenormWindowCache

| Aspect | DenormCache | DenormWindowCache |
|---|---|---|
| Implementation | `DataCache` — direct Redis lookup per event | Flink `MapState` with count/time window trigger |
| When used | Individual event enrichment (primary path) | Batch/window-based enrichment for high-throughput paths |
| State backend | Redis HGETALL per call | Flink state backend (checkpoint-backed) |
| Latency | Per-event Redis RTT | Amortized over window |
| Use case | Default denorm path | Summary events or high-cardinality lookups |

---

### 4.5 SummaryDenormalizationStreamTask

Separate Flink job for `SUMMARY` events (pre-aggregated data from the mobile app batch sync). Follows same enrichment steps as `DenormalizationFunction` but sources from `kafkaSummaryRouteTopic`. Output goes to `kafkaSummaryDenormSuccessTopic`. Failed events go to `kafkaDenormFailedTopic`.

---

## 5. DruidEventsValidator (Flink Job)

### 5.1 Stream Task Topology

```
kafkaDenormSuccessTopic (Event)
        |
        v
[DruidValidatorFunction]
        |-- telemetry valid  --> kafkaTelemetryRouteTopic (Druid telemetry datasource)
        |-- summary valid    --> kafkaSummaryRouteTopic   (Druid summary datasource)
        |-- invalid events   --> kafkaFailedTopic
        `-- error routing    --> kafkaErrorRouteTopic
```

### 5.2 Schema Validation Logic

The validator applies per-event-type JSON Schema validation:

1. Determine event type: `SUMMARY` events (from `eid == "ME_WORKFLOW_SUMMARY"` or similar) vs. telemetry events
2. Load the corresponding schema file from `config.schemaPath`
3. Validate event map against schema using `SchemaValidator`
4. If schema file missing: route to `failedEventsOutputTag` with `metadata.validation_err`
5. If validation fails: route to `failedEventsOutputTag`
6. If valid telemetry: emit to telemetry route topic
7. If valid summary: emit to summary route topic

**Schema sources:** Classpath resources under `config.schemaPath`. Each EID maps to a corresponding schema file (e.g., `telemetry.json`, `summary.json`).

**Flag set:** `dv_processed = true/false`, `dv_validation_status = "PASS"/"FAIL"`.

---

## 6. UserCacheUpdater v2 (Flink Job)

### 6.1 Trigger

**Trigger events:** `AUDIT` events where `edata.state == "Update"` and the object type relates to user profile changes (user creation, profile update, org membership change).

Specifically: `event.eid() == "AUDIT"` AND `event.objectType() == "User"`.

### 6.2 Cassandra Query

**Table:** `sunbird.user` (and related tables: `sunbird.user_org`, `sunbird.user_declarations`, `sunbird.usr_external_identity`)

**Columns fetched:**

| Column | Mapped field |
|---|---|
| `id` | User ID |
| `username` | Username |
| `rootorgid` | Root org ID |
| `framework` | Framework JSON (board/grade/subject/medium) |
| `profileusertypes` | User type list |
| `locationids` | Location ID list |
| `roles` | User roles |
| `status` | User account status |

Cassandra access via `CassandraUtil.findOne("SELECT ... FROM sunbird.user WHERE id = ?")`.

### 6.3 Redis Key Pattern and TTL

| Property | Value |
|---|---|
| Redis instance | Primary (`redisHost`/`redisPort`) |
| Redis DB | `redis.database.userstore.id` |
| Key | `user:<userId>` or plain `<userId>` (hash) |
| Command | `HMSET <userId> <field> <value> ...` |
| TTL | No explicit TTL — user cache is persistent |

### 6.4 UserMetadataUpdater Fields Mapped to Redis Hash

| Redis Hash Field | Source |
|---|---|
| `usersignintype` | Derived from `profileusertypes` |
| `userlogintype` | Derived from `profileusertypes` |
| `framework.board` | `framework.board[0]` |
| `framework.gradeLevel` | `framework.gradeLevel` |
| `framework.subject` | `framework.subject` |
| `framework.medium` | `framework.medium` |
| `framework.id` | `framework.id[0]` |
| `state` | Resolved from `locationids` via location lookup |
| `district` | Resolved from `locationids` via location lookup |

---

## 7. DeviceProfileUpdater (Flink Job)

### 7.1 Trigger Events

Events with `event.did()` non-null, typically all telemetry events that carry device context. The updater listens on a dedicated topic (post-denorm or from the device registry update path).

The function is triggered by events coming from the API layer when a device registers or updates its profile.

### 7.2 Cassandra Write

**Table:** `sunbird_device_registry.device_profile`

**Columns written:**

| Column | Source |
|---|---|
| `device_id` | `event.did()` |
| `api_last_updated_on` | Current timestamp |
| `channel` | `event.channel()` |
| `device_spec` | Device specification JSON |
| `state` | Resolved state name |
| `district` | Resolved district name |
| `uaspec` | User agent specification |

Cassandra write via `CassandraUtil.upsert(INSERT INTO ... IF NOT EXISTS / UPDATE ...)`.

### 7.3 Redis Update Pattern

| Property | Value |
|---|---|
| Redis instance | Primary |
| Redis DB | `redis.database.devicestore.id` |
| Key | `device:<deviceId>` or plain `<deviceId>` (hash) |
| Command | `HMSET <deviceId> <field> <value> ...` |
| Fields | `state`, `district`, `city`, `devicespec`, `os`, `make` |

---

## 8. ContentCacheUpdater (Flink Job)

### 8.1 Stream Task Topology

```
kafkaContentTopic (Map[String, AnyRef])
        |
        v
[ContentUpdaterFunction]  -- for content/collection events
        |
        v
[DialCodeUpdaterFunction] -- for dialcode events
        |
        v
Redis cache writes
```

### 8.2 ContentUpdaterFunction

**Trigger:** Events with object type `Content`, `TextBook`, `Collection`, `Course`, `TextBookUnit`, etc.

**Logic:**
1. Extract `objectId` and `objectType` from event
2. Fetch content metadata from Content Service REST API
3. Write returned metadata to Redis hash

**REST endpoint called:**
- `GET /api/content/v1/read/<contentId>?fields=<fieldList>` via `RestUtil.get(url, headers)`
- `GET /api/collection/v1/read/<collectionId>` for collections

**Redis writes:**

| Property | Value |
|---|---|
| Redis instance | Secondary (`metaRedisHost`/`metaRedisPort`) |
| Redis DB | `redis-meta.database.contentstore.id` |
| Key | `<contentId>` |
| Command | `HMSET <contentId> <field> <value> ...` |
| Fields | `name`, `objectType`, `contentType`, `mediaType`, `language`, `medium`, `gradeLevel`, `subject`, `board`, `status`, `channel` |

### 8.3 DialCodeUpdaterFunction

**Trigger:** Events with object type `DialCode` or `qr`.

**REST endpoint called:**
- `GET /api/dialcode/v1/read/<dialcode>` via `RestUtil.get(url, headers)`

Response is deserialized using `DialCodeResult` case class (defined in `RestUtil.scala`).

**Redis writes:**

| Property | Value |
|---|---|
| Redis instance | Secondary (`metaRedisHost`/`metaRedisPort`) |
| Redis DB | `redis-meta.database.dialcodestore.id` |
| Key | `<dialcode>` (uppercased) |
| Command | `HMSET <dialcode> <field> <value> ...` |
| Fields | `identifier`, `channel`, `batchCode`, `status` |

---

## 9. AssessmentAggregator (Flink Job)

### 9.1 Trigger Events

**Input:** `ASSESS` events from `kafkaAssessTopic` (post-denorm, validated).

Condition for processing: `event.eid() == "ASSESS"` with non-null `event.objectID()` (content ID) and `event.actorId()` (user ID).

### 9.2 AssessmentAggregatorFunction

**Class:** `AssessmentAggregatorFunction extends BaseProcessFunction[Event, Event]`

**Score computation per ASSESS event:**
1. Extract `edata.item.id` (question ID), `edata.item.maxscore`, `edata.item.score`
2. Extract `edata.resvalues` (list of user responses)
3. Calculate: `score = edata.score / edata.maxscore * 100` (percentage)
4. Accumulate per `(userId, courseId, batchId, contentId)` composite key

### 9.3 UserScoreAggregateFunction

**Class:** `UserScoreAggregateFunction extends WindowBaseProcessFunction[Event, Event, String]`

**Window type:** Global window with count trigger (configurable, e.g., every N events) or time-based trigger.

**Aggregation logic per window:**
1. Group events by `(userId, contentId, attemptId)`
2. For each group: compute `totalMaxScore = sum(item.maxscore)`, `totalScore = sum(item.score)`
3. Derive `pass = totalScore >= config.passCriteria`
4. Compute `percentScore = (totalScore / totalMaxScore) * 100`

### 9.4 Cassandra Writes

**Table:** `sunbird_courses.assessment_aggregator`

**Schema:**

| Column | Type | Value |
|---|---|---|
| `user_id` | text | `event.actorId()` |
| `course_id` | text | `event.objectID()` |
| `batch_id` | text | From `event.context.cdata` |
| `content_id` | text | From `edata.item.id` context |
| `attempt_id` | text | Derived UUID per attempt |
| `last_attempted_on` | timestamp | `event.ets()` |
| `score_achieved` | double | `sum(score)` |
| `max_score` | double | `sum(maxscore)` |
| `pass` | boolean | `totalScore >= passCriteria` |
| `total_score` | double | Percentage |
| `question` | list<frozen<map>> | Raw question+response data |
| `updated_on` | timestamp | Write time |

Write via `CassandraUtil.upsert()` with `IF NOT EXISTS` or `UPDATE`.

---

## 10. CBPreprocessor (Flink Job)

### 10.1 Purpose

CB = iGOT Competency-Based platform (formerly Karma Bharati / iGOT). Handles events specific to the iGOT learning platform that require specialized preprocessing before denormalization. Events are identified by `event.eid() == "CB_AUDIT"` or specific `producerId` values from iGOT producers.

### 10.2 CBEventsFlattener

**What gets flattened:** `CB_AUDIT` events that carry multiple competency updates in a single event payload (`edata.competencies` list or similar). Each competency entry is expanded into a separate event to enable per-competency analytics in Druid.

**Flattening logic:**
1. Read `edata` array of competency/course progress objects
2. For each item: create a new event copying the parent event's `actor`, `context`, `object`, `ets`, `mid`
3. Set `edata` of new event to the single item
4. Set `mid = original_mid + "_" + index` to ensure uniqueness
5. Emit each flattened event to the primary output

**Output tag:** `cbAuditRouteEventsOutputTag` (routed to `kafkaCbAuditRouteTopic`)

### 10.3 UserCacheUtil

**Purpose:** Enriches CB events with user profile data from Redis before routing.

**Cache lookup:**
- Redis DB: `redis.database.userstore.id`
- Key: `event.actorId()`
- Command: `HGETALL <userId>`
- Fields extracted: `rootorgid`, `framework`, `profileusertypes`

**Data written to event:** `event.userdata` (same structure as denorm user enrichment).

### 10.4 Output Structure

Flattened CB events follow the standard telemetry envelope with:
- `eid = "CB_AUDIT"`
- `edata` containing a single competency/progress record
- `mid` suffixed with positional index
- Full user context from `UserCacheUtil`

Routed to `kafkaCbAuditRouteTopic` which feeds the CB-specific Druid datasource.

---

## 11. IngestRouter (Flink Job)

### 11.1 Routing Logic

**Class:** `IngestRouterStreamTask`

**Input:** Raw events from the ingest API — can be either individual telemetry events or batch envelopes.

**Routing decision:**

| Condition | Output Topic |
|---|---|
| Event has `"events"` array field at root | `kafkaBatchRouteTopic` → TelemetryExtractor |
| Event is a single flat telemetry object with `"eid"` at root | `kafkaRawTopic` → PipelinePreprocessor directly |
| Event fails both checks | `kafkaFailedTopic` |

**Detection logic:** Inspect parsed JSON top-level keys. Presence of `"events"` (array) indicates a batch envelope from the mobile SDK. Presence of `"eid"` at root indicates an already-extracted individual event (from server-side producers).

### 11.2 Output Topics

| Topic Config Key | Content |
|---|---|
| `kafka.output.batch.route.topic` | Batch envelopes → TelemetryExtractor input |
| `kafka.output.raw.route.topic` | Individual events → PipelinePreprocessor input |
| `kafka.output.failed.topic` | Unparseable or unroutable events |

---

## 12. Rating (Flink Job)

### 12.1 RatingFunction

**Class:** `RatingFunction extends BaseProcessFunction[Event, Event]`

**Input events:** `FEEDBACK`-type events (or platform-specific rating events) where `event.edata` contains a rating score and the rated object identifier.

**Rating computation:**
1. Extract `event.objectID()` (rated content/course ID), `event.objectType()`, `event.actorId()` (user giving the rating)
2. Extract `edata.rating` (numeric, typically 1–5)
3. Look up existing rating aggregate from Cassandra for `(userId, activityId)`
4. Compute updated aggregate: count, sum of ratings, average
5. Write updated aggregate back to Cassandra
6. Emit enriched event downstream

### 12.2 Input Events and Output

**Input topic:** `kafkaRatingTopic` — events with rating/feedback payloads.

**Cassandra table:** `sunbird_courses.ratings` or `sunbird_courses.rating_summary`

**Columns:**

| Column | Value |
|---|---|
| `activity_id` | `event.objectID()` |
| `activity_type` | `event.objectType()` |
| `user_id` | `event.actorId()` |
| `rating` | `edata.rating` |
| `review` | `edata.review` (text) |
| `created_at` | `event.ets()` |
| `updated_at` | Current timestamp |

**Output topic:** `kafkaRatingSuccessTopic` — enriched events with rating metadata, consumed by Druid for rating analytics.

---

## 13. Data Models

### 13.1 Telemetry Event JSON Structure

```json
{
  "eid": "INTERACT",
  "mid": "UUID-v4-message-id",
  "ets": 1672531200000,
  "ver": "3.0",
  "syncts": 1672531200123,
  "@timestamp": "2023-01-01T00:00:00.123Z",
  "actor": {
    "id": "user-uuid",
    "type": "User"
  },
  "context": {
    "channel": "channel-id",
    "env": "home",
    "sid": "session-uuid",
    "did": "device-uuid",
    "pdata": {
      "id": "producer.id",
      "pid": "plugin.id",
      "ver": "1.0"
    },
    "cdata": [{ "type": "batch", "id": "batch-id" }],
    "rollup": { "l1": "collection-id" }
  },
  "object": {
    "id": "content-uuid",
    "type": "Content",
    "ver": "1.0",
    "rollup": { "l1": "parent-id" }
  },
  "edata": {
    "type": "click",
    "subtype": "launch",
    "id": "element-id",
    "pageid": "page-id"
  },
  "tags": ["tag1"],
  "flags": {
    "ex_processed": true,
    "pp_validation_processed": true,
    "pp_duplicate": false
  },
  "metadata": {
    "checksum": "md5-or-sha-hash",
    "src": "pipeline-job-name"
  }
}
```

---

### 13.2 Batch Envelope Structure (from API / Mobile SDK)

```json
{
  "id": "api.sunbird.telemetry",
  "ver": "3.0",
  "params": {
    "msgid": "batch-uuid",
    "ver": "3.0",
    "sync_status": "SUCCESS"
  },
  "ets": 1672531200000,
  "events": [
    { "eid": "START", "mid": "...", "ets": 1672531100000 },
    { "eid": "INTERACT", "mid": "...", "ets": 1672531110000 }
  ],
  "context": {
    "channel": "channel-id",
    "env": "home",
    "did": "device-uuid",
    "pdata": { "id": "org.sunbird.app", "ver": "4.0.0", "pid": "" }
  }
}
```

The `params.msgid` is the deduplication key in `DeduplicationFunction`. The `events` array is the extraction target in `ExtractionFunction`.

---

### 13.3 Denormalized Event Additional Fields

After denormalization, events carry these additional top-level blobs (see `EventsPath` constants):

```json
{
  "devicedata": {
    "os": "Android 11",
    "make": "Samsung SM-A505F",
    "id": "device-uuid",
    "location": "Bengaluru",
    "stateName": "Karnataka",
    "districtName": "Bengaluru Urban",
    "devicespec": { "cpu": "...", "mem": "...", "scrn": "..." }
  },
  "userdata": {
    "usersignintype": "Self-Signed-In",
    "userlogintype": "Student",
    "framework": {
      "board": "CBSE",
      "gradeLevel": ["Class 5"],
      "subject": ["Mathematics"],
      "medium": ["English"],
      "id": ["cbse_k-12"]
    }
  },
  "contentdata": {
    "name": "Content Title",
    "objectType": "Content",
    "contentType": "Resource",
    "mediaType": "content",
    "language": ["English"],
    "medium": ["English"],
    "gradeLevel": ["Class 5"],
    "subject": ["Mathematics"],
    "board": "CBSE",
    "status": "Live",
    "channel": "channel-id"
  },
  "dialcodedata": {
    "identifier": "QR1234",
    "channel": "channel-id",
    "batchCode": "batch-2023"
  },
  "derivedlocationdata": {
    "state": "Karnataka",
    "district": "Bengaluru Urban",
    "from": "device"
  },
  "collectiondata": {},
  "l2data": {}
}
```

---

## 14. Configuration Reference

### 14.1 Key Config Properties Per Job

#### TelemetryExtractor

| Property | Typical Value | Purpose |
|---|---|---|
| `task.consumer.parallelism` | 1–4 | Kafka source parallelism |
| `task.downstream.operators.parallelism` | 1–8 | Function operator parallelism |
| `kafka.input.topic` | `telemetry.ingest` | Batch event input |
| `kafka.groupid` | `telemetry-extractor-group` | Consumer group |
| `redis.database.duplicationstore.id` | `12` | Dedup Redis DB index |
| `redis.database.key.expiry.seconds` | `3600` | Dedup TTL (seconds) |
| `redis-meta.database.contentstore.id` | `5` | Content/question metadata DB |
| `kafka.event.max.size` | `1048576` (1MB) | Per-event size limit |
| `redact.events.list` | `["ASSESS","RESPONSE"]` | EIDs requiring redaction check |
| `job.checkpointing.interval` | `30000` | Checkpoint every 30s |

#### PipelinePreprocessor

| Property | Typical Value | Purpose |
|---|---|---|
| `task.consumer.parallelism` | 1–4 | Kafka source parallelism |
| `task.downstream.operators.parallelism` | 1–8 | Function parallelism |
| `kafka.input.topic` | `telemetry.raw` | Extracted individual events |
| `kafka.groupid` | `pipeline-preprocessor-group` | Consumer group |
| `redis.database.duplicationstore.id` | `12` | Dedup Redis DB index |
| `redis.database.key.expiry.seconds` | `3600` | Dedup TTL |
| `telemetry.schema.path` | `/schemas/telemetry/` | JSON schema base path |
| `dedup.producer.included.ids` | `["prod.sunbird.app"]` | Producers requiring dedup (currently unused — hardcoded true) |
| `secondary.events` | `["SEARCH"]` | EIDs routed to secondary denorm topic |

#### Denormalization

| Property | Typical Value | Purpose |
|---|---|---|
| `kafka.input.topic` | `telemetry.denorm` | Post-preprocessor events |
| `kafka.groupid` | `denorm-group` | Consumer group |
| `redis.database.userstore.id` | `4` | User data Redis DB |
| `redis.database.devicestore.id` | `2` | Device data Redis DB |
| `redis-meta.database.contentstore.id` | `5` | Content data Redis DB |
| `redis-meta.database.dialcodestore.id` | `6` | Dialcode Redis DB |
| `redis.database.locationstore.id` | `3` | Location Redis DB |

#### UserCacheUpdater v2

| Property | Typical Value | Purpose |
|---|---|---|
| `kafka.input.topic` | `telemetry.audit` | AUDIT events input |
| `kafka.groupid` | `user-cache-updater-group` | Consumer group |
| `cassandra.host` | `cassandra.host` | Cassandra contact point |
| `cassandra.port` | `9042` | Cassandra port |
| `redis.database.userstore.id` | `4` | User cache Redis DB |
| `kafka.output.failed.topic` | `telemetry.failed` | Failed event routing |

#### AssessmentAggregator

| Property | Typical Value | Purpose |
|---|---|---|
| `kafka.input.topic` | `telemetry.assess` | ASSESS events input |
| `kafka.groupid` | `assessment-aggregator-group` | Consumer group |
| `cassandra.host` | Cassandra host | Aggregation writes |
| `cassandra.keyspace` | `sunbird_courses` | Keyspace |
| `cassandra.table` | `assessment_aggregator` | Aggregation table |
| `pass.criteria.percentage` | `70` | Minimum pass score |
| `job.checkpointing.interval` | `60000` | Checkpoint every 60s |

#### Global / All Jobs

| Property | Purpose |
|---|---|
| `kafka.producer.broker-servers` | Kafka producer bootstrap servers |
| `kafka.consumer.broker-servers` | Kafka consumer bootstrap servers |
| `kafka.producer.compression` | Default: `snappy` |
| `redis.host` / `redis.port` | Primary Redis (user, device, dedup) |
| `redis-meta.host` / `redis-meta.port` | Secondary Redis (content, dialcode) |
| `redis.connection.timeout` | Default: `30000` ms |
| `task.restart-strategy.attempts` | Default: `3` |
| `task.restart-strategy.delay` | Default: `30000` ms |
| `job.enable.distributed.checkpointing` | `true` for production |
| `job.statebackend.base.url` | GCS/HDFS/S3 path for FsStateBackend |
