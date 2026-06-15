# Sunbird Data Pipeline - Practical Code Guide & Examples

---

## Table of Contents

1. [Running a Flink Job](#running-a-flink-job)
2. [Event Structure Examples](#event-structure-examples)
3. [Configuration Examples](#configuration-examples)
4. [Common Code Patterns](#common-code-patterns)
5. [Debugging Guide](#debugging-guide)
6. [Development Workflow](#development-workflow)

---

## Running a Flink Job

### Prerequisites

```bash
# Requirements
- Java 8 or 11
- Scala 2.11 or 2.12
- Maven 3.6+
- Flink 1.10+
- Kafka running
- Redis running
```

### Build the Project

```bash
# Build all modules
cd /path/to/sunbird-data-pipeline
mvn clean package -DskipTests

# Build specific job
cd data-pipeline-flink/telemetry-extractor
mvn clean package -DskipTests
```

### Submit Job to Flink

#### Local Development

```bash
# Start local Flink environment
$FLINK_HOME/bin/start-cluster.sh

# Submit job
$FLINK_HOME/bin/flink run -c org.sunbird.dp.extractor.task.TelemetryExtractorStreamTask \
  data-pipeline-flink/telemetry-extractor/target/telemetry-extractor-*.jar \
  --config /path/to/application.conf

# Monitor
$FLINK_HOME/bin/taskmanager.sh

# Stop
$FLINK_HOME/bin/stop-cluster.sh
```

#### Cluster Deployment (YARN)

```bash
# Submit to YARN
$FLINK_HOME/bin/flink run -m yarn-cluster \
  -c org.sunbird.dp.extractor.task.TelemetryExtractorStreamTask \
  -n 2 -tm 2048 -s 2 \
  data-pipeline-flink/telemetry-extractor/target/telemetry-extractor-*.jar \
  --config /path/to/application.conf

# Parameters:
# -n: number of task managers
# -tm: memory per task manager (MB)
# -s: slots per task manager
```

### Configuration Files

Each job reads configuration from `application.conf` (TypeSafe Config format):

```hocon
# Kafka Configuration
kafka {
  broker-servers = "localhost:9092"
  input {
    topic = "telemetry.ingest"
    parallelism = 4
  }
  output {
    success {
      topic = "telemetry.raw"
    }
    failed {
      topic = "telemetry.failed"
    }
  }
}

# Redis Configuration
redis {
  host = "localhost"
  port = 6379
  database {
    duplicationstore.id = 0
    key.expiry.seconds = 86400
  }
}

# Flink Task Configuration
task {
  consumer.parallelism = 4
  downstream.operators.parallelism = 8
  window.count = 100
}

# Application-specific
default.channel = "org.sunbird"
kafka.event.max.size = 1048576  # 1 MB
```

---

## Event Structure Examples

### 1. Batch Event (telemetry.ingest)

**Input from Telemetry API:**

```json
{
  "mid": "batch-id-12345",
  "params": {
    "msgid": "unique-batch-message-id-xyz",
    "did": "device-456",
    "consumer_id": "prod.diksha.portal"
  },
  "channel": "prod.diksha.portal",
  "syncts": 1745747445123,
  "events": [
    {
      "eid": "CLICK",
      "edata": {
        "id": "play-button",
        "type": "button",
        "pageid": "content-player"
      },
      "object": {
        "id": "content-xyz",
        "type": "Content",
        "rollup": {
          "l1": "course-123"
        }
      },
      "context": {
        "channel": "prod.diksha.portal",
        "pdata": {
          "id": "prod.diksha.portal",
          "ver": "4.2.0",
          "pid": "mobile"
        },
        "actor": {
          "id": "user-789",
          "type": "User"
        }
      }
    },
    {
      "eid": "VIEW",
      "edata": {
        "type": "player"
      },
      "object": {
        "id": "content-xyz",
        "type": "Content"
      },
      "context": { ... }
    }
  ]
}
```

### 2. Event After Telemetry Extractor

**Output to telemetry.raw:**

```json
{
  "eid": "CLICK",
  "edata": {
    "id": "play-button",
    "type": "button",
    "pageid": "content-player"
  },
  "object": {
    "id": "content-xyz",
    "type": "Content",
    "rollup": {
      "l1": "course-123"
    }
  },
  "context": {
    "channel": "prod.diksha.portal",
    "pdata": {
      "id": "prod.diksha.portal",
      "ver": "4.2.0",
      "pid": "mobile"
    },
    "actor": {
      "id": "user-789",
      "type": "User"
    }
  },
  "@timestamp": "2026-04-27T10:30:45.123Z",
  "syncts": 1745747445123,
  "mid": "batch-id-12345",
  "flags": {
    "ex_processed": true,
    "extractor_duplicate": false
  },
  "metadata": {
    "src": "telemetry-extractor"
  }
}
```

### 3. Event After Pipeline Preprocessor

**Output to telemetry.denorm (after validation & routing):**

```json
{
  ...previous fields...,
  "flags": {
    "ex_processed": true,
    "extractor_duplicate": false,
    "pp_validation_processed": true,
    "pp_duplicate": false,
    "pp_duplicate_skipped": false
  },
  "metadata": {
    "src": "pipeline-preprocessor"
  }
}
```

### 4. Event After De-normalization

**Output to telemetry.unique.primary (fully enriched):**

```json
{
  ...previous fields...,
  "devicedata": {
    "country": "India",
    "state": "Karnataka",
    "state_code": "KA",
    "city": "Bangalore",
    "district": "Bangalore",
    "state_code_custom": null,
    "state_custom": "Karnataka",
    "user_declared_state": "Karnataka",
    "user_declared_district": "Bangalore",
    "devicespec": {
      "os": "Android",
      "cpu": "8",
      "mem": "4096"
    },
    "firstaccess": 1740000000000
  },
  "userdata": {
    "usertype": "teacher",
    "usersubtype": "state_resource_person",
    "grade": "8,9,10",
    "language": "en,hi",
    "subject": "Mathematics,Science",
    "state": "Karnataka",
    "district": "Bangalore",
    "usersignintype": "self-signed",
    "userlogintype": "google",
    "block": "Whitefield",
    "cluster": "Tech Cluster",
    "schoolname": "ABC Public School"
  },
  "contentdata": {
    "name": "Algebra Basics",
    "objectType": "Content",
    "contentType": "Course",
    "mediaType": "video",
    "language": "en",
    "medium": "English",
    "mimeType": "application/pdf",
    "createdBy": "author-123",
    "createdFor": ["b1", "b2"],
    "framework": "NIFT",
    "board": "CBSE",
    "subject": "Mathematics",
    "status": "Live",
    "pkgVersion": 1,
    "lastSubmittedOn": 1745000000000,
    "lastUpdatedOn": 1745100000000,
    "lastPublishedOn": 1745200000000,
    "channel": "org.sunbird",
    "gradeLevel": ["8", "9"],
    "keywords": ["algebra", "math"]
  },
  "collectiondata": {
    "name": "Mathematics - Grade 8",
    "contentType": "Collection",
    "mimeType": "application/pdf",
    "framework": "NIFT",
    "subject": "Mathematics",
    "medium": "English",
    "board": "CBSE",
    "channel": "org.sunbird",
    "createdFor": ["b1"],
    "gradeLevel": ["8"]
  },
  "dialcodedata": {
    "identifier": "ABC123DEF",
    "channel": "org.sunbird",
    "batchcode": "batch-2026-001",
    "publisher": "publisher-xyz",
    "generated_on": 1740000000,
    "published_on": 1740100000,
    "status": "published"
  },
  "derivedlocationdata": {
    "location": "Karnataka",
    "derivedFrom": "user_profile"
  },
  "flags": {
    "ex_processed": true,
    "pp_validation_processed": true,
    "pp_duplicate": false,
    "denorm_device": true,
    "denorm_user": true,
    "denorm_content": true,
    "denorm_collection": true,
    "denorm_dialcode": false,
    "denorm_location": true
  }
}
```

### 5. Event After Druid Validator (FINAL)

**Output to druid.events.telemetry (ready for Druid ingestion):**

```json
{
  ...all previous fields...,
  "flags": {
    ...all previous flags...,
    "dv_duplicate": false
  },
  "validated": true,
  "metadata": {
    "src": "druid-validator",
    "ingested_at": 1745747450000
  }
}
```

---

## Configuration Examples

### Telemetry Extractor Config

**File: `telemetry-extractor/src/main/resources/application.conf`**

```hocon
include "base-config"

kafka {
  broker-servers = ${kafka.broker.servers}
  zookeeper-servers = ${kafka.zookeeper.servers}
  
  input.topic = "telemetry.ingest"
  
  output {
    success.topic = "telemetry.raw"
    log.route.topic = "telemetry.log"
    duplicate.topic = "telemetry.duplicate"
    failed.topic = "telemetry.failed"
    batch.failed.topic = "telemetry.batch.failed"
    assess.raw.topic = "telemetry.assess.raw"
  }
  
  event.max.size = 1048576  # 1 MB
}

redis {
  host = ${redis.host}
  port = ${redis.port}
  database {
    duplicationstore.id = 0
    key.expiry.seconds = 86400  # 24 hours
  }
}

task {
  consumer.parallelism = 4
  downstream.operators.parallelism = 8
}

redact.events.list = [ASSESS, RESPONSE]

redis-meta {
  database {
    contentstore.id = 2
  }
  host = ${redis.host}
  port = ${redis.port}
}
```

### Pipeline Preprocessor Config

**File: `pipeline-preprocessor/src/main/resources/application.conf`**

```hocon
include "base-config"

kafka {
  broker-servers = ${kafka.broker.servers}
  
  input.topic = "telemetry.raw"
  
  output {
    primary.route.topic = "telemetry.denorm"
    log.route.topic = "telemetry.log"
    error.route.topic = "telemetry.error"
    audit.route.topic = "telemetry.audit"
    cb.audit.route.topic = "telemetry.cb.audit"
    failed.topic = "telemetry.failed"
    duplicate.topic = "telemetry.duplicate"
    
    denorm {
      primary.route.topic = "telemetry.denorm.primary"
      secondary.route.topic = "telemetry.denorm.secondary"
    }
  }
}

redis {
  host = ${redis.host}
  port = ${redis.port}
  database {
    duplicationstore.id = 0
    key.expiry.seconds = 86400
  }
}

task {
  consumer.parallelism = 4
  downstream.operators.parallelism = 8
}

telemetry.schema.path = "/schemas/telemetry/"
default.channel = "org.sunbird"

# Only deduplicate events from these producers
dedup.producer.included.ids = ["prod.diksha.portal", "prod.sunbird.desktop"]
```

### De-normalization Config

**File: `de-normalization/src/main/resources/application.conf`**

```hocon
include "base-config"

kafka {
  broker-servers = ${kafka.broker.servers}
  
  input {
    telemetry.topic = "telemetry.denorm.primary"
    summary.topic = "telemetry.denorm.secondary"
  }
  
  output {
    telemetry.denorm.output.topic = "telemetry.unique.primary"
    summary.denorm.output.topic = "telemetry.unique.secondary"
    failed.topic = "telemetry.failed"
  }
}

redis-meta {
  user {
    host = ${redis.meta.host}
    port = ${redis.meta.port}
  }
  device {
    host = ${redis.meta.host}
    port = ${redis.meta.port}
  }
  content {
    host = ${redis.meta.host}
    port = ${redis.meta.port}
  }
  dialcode {
    host = ${redis.meta.host}
    port = ${redis.meta.port}
  }
  
  database {
    userstore.id = 0
    devicestore.id = 1
    contentstore.id = 2
    dialcodestore.id = 3
  }
}

task {
  consumer.parallelism = 4
  telemetry.downstream.operators.parallelism = 8
  summary.downstream.operators.parallelism = 4
  window.count = 100
  window.shards = 4
}

# Events older than 3 months are discarded
telemetry.ignore.period.months = 3

# Summary events to process (others are skipped)
summary.filter.events = [ME_WORKFLOW_SUMMARY]

# Events to skip denormalization
skip.events = [LOG, ERROR]

user.signin.type.default = "Anonymous"
user.login.type.default = "NA"
```

### Druid Validator Config

**File: `druid-events-validator/src/main/resources/application.conf`**

```hocon
include "base-config"

kafka {
  broker-servers = ${kafka.broker.servers}
  
  input.topic = "telemetry.unique.primary"
  
  output {
    telemetry.route.topic = "druid.events.telemetry"
    summary.route.topic = "druid.events.summary"
    failed.topic = "telemetry.failed"
    duplicate.topic = "telemetry.duplicate"
  }
}

redis {
  host = ${redis.host}
  port = ${redis.port}
  database {
    duplicationstore.id = 0
    key.expiry.seconds = 86400
  }
}

task {
  consumer.parallelism = 4
  downstream.operators.parallelism = 8
  
  # Enable/disable validation and dedup
  druid.validation.enabled = true
  druid.deduplication.enabled = true
}

# Schema paths for final validation
schema {
  path {
    telemetry = "/schemas/druid/telemetry/"
    summary = "/schemas/druid/summary/"
  }
  file {
    default = "telemetry.json"
    search = "search.json"
    summary = "summary.json"
  }
}
```

---

## Common Code Patterns

### 1. BaseProcessFunction Pattern

Most Flink functions in this codebase extend `BaseProcessFunction`:

```scala
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}

class MyProcessFunction(config: MyConfig)(implicit val typeInfo: TypeInformation[MyType])
  extends BaseProcessFunction[InputType, OutputType](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MyProcessFunction])
  
  // Define metrics to track
  override def metricsList(): List[String] = {
    List("my-success-count", "my-failure-count")
  }
  
  // Initialize resources
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // Initialize Redis, caches, validators, etc.
  }
  
  // Clean up resources
  override def close(): Unit = {
    super.close()
    // Close Redis connections, etc.
  }
  
  // Main processing logic
  override def processElement(element: InputType,
                              context: ProcessFunction[InputType, OutputType]#Context,
                              metrics: Metrics): Unit = {
    try {
      // Process element
      val result = transform(element)
      
      // Output to main stream or side output
      context.output(config.outputTag, result)
      
      // Increment metrics
      metrics.incCounter("my-success-count")
    } catch {
      case ex: Exception =>
        logger.error("Error processing element", ex)
        metrics.incCounter("my-failure-count")
        // Output to failed topic or re-throw
        context.output(config.failedOutputTag, element)
    }
  }
  
  private def transform(element: InputType): OutputType = {
    // Business logic
    element.asInstanceOf[OutputType]
  }
}
```

### 2. Deduplication Pattern

The framework provides a reusable deduplication function:

```scala
import org.sunbird.dp.core.cache.DedupEngine

// Inside your ProcessFunction
def processElement(event: String, context, metrics): Unit = {
  val eventId = extractId(event)  // Get unique identifier (msgid, mid, etc.)
  
  // Call deDup helper function
  deDup[String, Map[String, AnyRef]](
    eventId,                           // Unique ID
    event,                             // Event data
    context,                           // Flink context for side outputs
    config.uniqueEventOutputTag,       // Tag for unique events
    config.duplicateEventOutputTag,    // Tag for duplicates
    flagName = "my_duplicate"          // Flag to mark duplicates
  )(dedupEngine, metrics)  // Pass dedupEngine and metrics
}

// Behind the scenes, deDup:
// 1. Checks if eventId exists in Redis
// 2. If NOT → Adds to Redis, outputs to uniqueEventOutputTag
// 3. If YES → Outputs to duplicateEventOutputTag, increments duplicate counter
```

### 3. Schema Validation Pattern

```scala
import org.sunbird.dp.core.util.SchemaValidator

class MyValidationFunction(config: Config,
                          @transient var schemaValidator: SchemaValidator = null)
  extends BaseProcessFunction[Event, Event](config) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (schemaValidator == null) {
      schemaValidator = new SchemaValidator(config)
    }
  }

  override def processElement(event: Event, context, metrics): Unit = {
    // Check if schema exists for this event type
    val isSchemaPresent = schemaValidator.schemaFileExists(event)
    
    if (!isSchemaPresent) {
      // Handle missing schema
      context.output(config.failedOutputTag, event)
      return
    }
    
    // Validate event against schema
    val validationReport = schemaValidator.validate(event, isSchemaPresent)
    
    if (validationReport.isSuccess) {
      // Event is valid
      context.output(config.validEventOutputTag, event)
      metrics.incCounter(config.validationSuccessCount)
    } else {
      // Event failed validation
      val errorMessage = schemaValidator.getInvalidFieldName(validationReport.toString)
      event.markValidationFailure(errorMessage)
      context.output(config.failedOutputTag, event)
      metrics.incCounter(config.validationFailureCount)
    }
  }
}
```

### 4. Redis Cache Pattern

```scala
import org.sunbird.dp.core.cache.RedisConnect

class MyCacheFunction(config: Config) extends BaseProcessFunction[Event, Event](config) {
  
  @transient private var redisConnect: RedisConnect = _
  
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
  }
  
  override def close(): Unit = {
    super.close()
    redisConnect.close()
  }
  
  override def processElement(event: Event, context, metrics): Unit = {
    try {
      val cacheKey = event.getDeviceId()
      
      // Get data from Redis
      val cachedData = redisConnect.hgetAll(cacheKey)
      
      if (cachedData != null && !cachedData.isEmpty) {
        // Cache HIT
        event.enrichWithData(cachedData)
        metrics.incCounter("cache-hit")
      } else {
        // Cache MISS
        logger.warn(s"Cache miss for key: $cacheKey")
        metrics.incCounter("cache-miss")
      }
      
      context.output(config.outputTag, event)
      
    } catch {
      case ex: JedisException =>
        logger.error("Redis connection error", ex)
        // Stop job on Redis error (fail-fast)
        throw ex
    }
  }
}
```

### 5. Window Aggregation Pattern

```scala
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.functions.windowing.WindowFunction

class MyWindowFunction(config: Config) 
  extends WindowFunction[Event, AggregatedEvent, String, GlobalWindow] {
  
  override def apply(key: String,
                     window: GlobalWindow,
                     input: Iterable[Event],
                     out: Collector[AggregatedEvent]): Unit = {
    
    var count = 0
    var totalDuration = 0L
    
    input.forEach(event => {
      count += 1
      totalDuration += event.getDuration()
    })
    
    val aggregated = AggregatedEvent(
      key = key,
      eventCount = count,
      avgDuration = totalDuration / count,
      timestamp = System.currentTimeMillis()
    )
    
    out.collect(aggregated)
  }
}

// Usage in stream:
stream
  .keyBy(event => event.groupId)
  .countWindow(100)  // Aggregate every 100 events
  .apply(new MyWindowFunction(config))
  .addSink(kafkaSink)
```

---

## Debugging Guide

### 1. View Kafka Topics

```bash
# List all topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe a topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic telemetry.raw

# Read messages from topic (from beginning)
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.raw --from-beginning --max-messages 10

# Read latest messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.raw --tail 20

# Get consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group telemetry-extractor-consumer --describe
```

### 2. Check Redis

```bash
# Connect to Redis
redis-cli -h localhost -p 6379

# Check database 0 (dedup store)
SELECT 0

# Count keys
DBSIZE

# Check if specific message ID exists
EXISTS "msg-id-xyz"

# Get data
GET "msg-id-xyz"

# Check key expiration
TTL "msg-id-xyz"

# Get all keys (use with caution on large datasets)
KEYS "*" | head -20

# Monitor Redis commands in real-time
MONITOR
```

### 3. Flink Web UI

```
# Access at: http://localhost:8081

# Check:
- ├─ Task Managers: Health status of parallel workers
- ├─ Jobs: Running/completed job status
- ├─ TaskManager Logs: Real-time logs
- ├─ Metrics: Performance metrics
- └─ Configuration: Job settings
```

### 4. Check Job Logs

```bash
# Flink job manager logs
tail -f $FLINK_HOME/log/flink-*.log

# Grep for errors
grep -i error $FLINK_HOME/log/flink-*.log

# Search for specific function
grep "TelemetryExtractorStreamTask" $FLINK_HOME/log/flink-*.log

# Follow logs in real-time
tail -f $FLINK_HOME/log/flink-*.log | grep -i "error\|exception"
```

### 5. Debug Event Flow

**To trace a specific event through the pipeline:**

```bash
# 1. Get event from input topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.ingest --from-beginning --max-messages 1 > /tmp/event.json

# 2. Check if it's in failed topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.failed --from-beginning | grep -i "event-id"

# 3. Check if it's duplicated
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.duplicate --from-beginning | grep -i "msg-id"

# 4. Check final output
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic druid.events.telemetry --from-beginning | grep -i "event-id"
```

### 6. Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| **High duplicate count** | Same batch/event sent multiple times | Check Kafka producer retry settings |
| **Cache misses increasing** | Device/user/content not in Redis | Update cache update job, check Redis memory |
| **Validation failures** | Event schema mismatch | Check schema files, verify event structure |
| **Job fails on Redis error** | Redis connection lost | Restart Redis, check network connectivity |
| **Lag increasing** | Processing slower than ingestion rate | Increase task parallelism, scale up cluster |
| **Out of memory** | Too many events buffered | Reduce window size, increase heap memory |

---

## Development Workflow

### Setting Up Development Environment

```bash
# 1. Clone repository
git clone <repo-url>
cd sunbird-data-pipeline

# 2. Build project
mvn clean install -DskipTests

# 3. Create IDE configuration
mvn idea:idea  # For IntelliJ
mvn eclipse:eclipse  # For Eclipse
mvn compile  # For VS Code + Metals

# 4. Start local services
# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

# Start Redis
redis-server

# Start Flink (optional)
$FLINK_HOME/bin/start-cluster.sh

# 5. Run tests
mvn test -pl data-pipeline-flink/telemetry-extractor
```

### Creating a New Flink Job

```scala
// 1. Create domain model
// src/main/scala/org/sunbird/dp/newjob/domain/Event.scala
case class Event(
  eid: String,
  edata: Map[String, Any],
  context: Map[String, Any],
  flags: Map[String, Boolean]
)

// 2. Create config
// src/main/scala/org/sunbird/dp/newjob/task/NewJobConfig.scala
class NewJobConfig(override val config: Config) extends BaseJobConfig(config, "NewJobName") {
  val kafkaInputTopic = config.getString("kafka.input.topic")
  val kafkaOutputTopic = config.getString("kafka.output.topic")
  // ... more config
}

// 3. Create processing function
// src/main/scala/org/sunbird/dp/newjob/functions/NewJobFunction.scala
class NewJobFunction(config: NewJobConfig)(implicit val typeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {
  
  override def processElement(event: Event, context, metrics) = {
    // Process logic
    context.output(config.outputTag, event)
  }
}

// 4. Create stream task
// src/main/scala/org/sunbird/dp/newjob/task/NewJobStreamTask.scala
class NewJobStreamTask(config: NewJobConfig, kafkaConnector: FlinkKafkaConnector) {
  def process() = {
    val env = FlinkUtil.getExecutionContext(config)
    
    env.addSource(kafkaConnector.kafkaEventSource(config.kafkaInputTopic))
      .process(new NewJobFunction(config))
      .addSink(kafkaConnector.kafkaEventSink(config.kafkaOutputTopic))
    
    env.execute(config.jobName)
  }
}

// 5. Create application.conf
// src/main/resources/application.conf
kafka {
  input.topic = "input-topic"
  output.topic = "output-topic"
}
```

### Testing Strategy

```scala
// Unit test with MockingFramework
class NewJobFunctionTest extends FlatSpec with Matchers {
  
  "NewJobFunction" should "process event correctly" in {
    val config = mock[NewJobConfig]
    val function = new NewJobFunction(config)
    
    val event = Event(eid = "CLICK", edata = Map(), ...)
    val result = function.processElement(event, ...)
    
    result should be (expectedResult)
  }
}

// Integration test with embedded Kafka
class NewJobStreamTaskTest extends StreamingTest {
  
  "Stream task" should "consume from input and produce to output" in {
    val task = new NewJobStreamTask(config, kafkaConnector)
    
    // Insert test data to input topic
    publishToKafka("input-topic", testEvent)
    
    // Run stream task
    task.process()
    
    // Verify output
    val result = consumeFromKafka("output-topic")
    result should contain(expectedEvent)
  }
}
```

### Code Review Checklist

- [ ] All metrics are incremented appropriately
- [ ] Error handling is in place (try-catch, fail-fast for Redis)
- [ ] Side outputs are used correctly for routing
- [ ] Flags are set to track event processing history
- [ ] Configuration is externalized (no hardcoded values)
- [ ] Logging is comprehensive for debugging
- [ ] Tests cover happy path and error scenarios
- [ ] Documentation is updated

---

**End of Practical Code Guide**

For more information, refer to:
- Apache Flink Documentation: https://nightlies.apache.org/flink/flink-docs-stable/
- Scala Documentation: https://docs.scala-lang.org/
- Kafka Documentation: https://kafka.apache.org/documentation/
