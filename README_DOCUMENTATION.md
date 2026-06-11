# Sunbird Data Pipeline - Quick Reference & Summary

---

## 📋 Documentation Index

This documentation package contains:

1. **[ARCHITECTURE_GUIDE.md](ARCHITECTURE_GUIDE.md)** - START HERE
   - High-level architecture overview
   - Data flow diagram
   - Key concepts (deduplication, denormalization, event types)
   - Detailed deep dive into each job with code samples
   - Technology stack
   - Data quality & resilience

2. **[SEQUENCE_DIAGRAM.md](SEQUENCE_DIAGRAM.md)** - VISUAL REFERENCE
   - Step-by-step sequence of how data flows through pipeline
   - Event state transformations at each stage
   - Error scenarios & routing decisions
   - Performance characteristics & metrics

3. **[PRACTICAL_CODE_GUIDE.md](PRACTICAL_CODE_GUIDE.md)** - HANDS-ON
   - How to run Flink jobs
   - Real event structure examples (JSON)
   - Configuration examples for each job
   - Common code patterns used in codebase
   - Debugging guide & troubleshooting
   - Development workflow

---

## 🏗️ Architecture at a Glance

```
Telemetry API
    ↓
telemetry.ingest (Kafka Topic)
    ↓
[Telemetry Extractor Job] → Dedup batch, unpack events
    ↓
telemetry.raw (Kafka Topic)
    ↓
[Pipeline Preprocessor Job] → Validate schema, dedup events, route
    ↓
telemetry.denorm.primary/secondary (Kafka Topics)
    ↓
[De-normalization Job] → Enrich with device/user/content/location data
    ↓
telemetry.unique.primary/secondary (Kafka Topics)
    ↓
[Druid Events Validator Job] → Final validation, dedup summaries
    ↓
druid.events.telemetry (Kafka Topic)
    ↓
DRUID DATABASE ← Analytics dashboards (Metabase, Grafana, etc.)
    ↓
CLOUD STORAGE (Archive via Secor)
```

---

## 📊 Job Comparison Matrix

| Feature | Telemetry Extractor | Pipeline Preprocessor | De-normalization | Druid Validator |
|---------|-------------------|----------------------|------------------|-----------------|
| **Input** | telemetry.ingest | telemetry.raw | telemetry.denorm.* | telemetry.unique.* |
| **Output** | telemetry.raw | telemetry.denorm.* | telemetry.unique.* | druid.events.telemetry |
| **Primary Function** | Batch unpacking | Schema validation | Data enrichment | Final validation |
| **Dedup Strategy** | By batch msgid | By event mid | N/A | By summary mid |
| **Cache Usage** | Minimal | Schema only | Extensive (device/user/content) | Schema only |
| **Processing Time** | ~0.1ms/event | ~1ms/event | ~5ms/event | ~0.5ms/event |
| **Parallelism** | 4 consumers | 4 consumers | 4 consumers | 4 consumers |
| **Redis Impact** | Low (dedup only) | Low (dedup only) | Very high (enrichment) | Low (dedup only) |
| **Failure Handling** | Send to failed topic | Send to failed topic | Fail-fast (Redis critical) | Send to failed topic |

---

## 🔑 Key Concepts Quick Reference

### Deduplication
- **Storage**: Redis DB 0
- **Keys**: msgid (batch level), mid (event level)
- **TTL**: 24 hours (configurable)
- **Impact**: First occurrence unique, subsequent duplicates

### Event Routing
| Event Type | Processing | Output |
|-----------|-----------|--------|
| LOG | Skip denorm | telemetry.log |
| ERROR | Skip denorm | telemetry.error |
| AUDIT | Include in denorm | telemetry.audit + denorm |
| SHARE | Flatten to items | telemetry.denorm |
| Regular | Full denorm | telemetry.denorm |
| Summary | Full process | druid.events.summary |

### Denormalization Data

| Cache | Data Examples | Impact on Queries |
|-------|--------------|-------------------|
| **Device** | Country, state, city, device specs | Geo-analytics, device trends |
| **User** | Profile, grade, subject, language | User behavior, learning paths |
| **Content** | Name, type, subject, board, status | Content performance, adoption |
| **DialCode** | QR info, publisher, dates | QR code usage analytics |
| **Location** | Derived from user/device with fallback | Location-based insights |

### Event Flags (Processing History)

```
ex_processed ........................ Extractor processed
extractor_duplicate ................ Duplicate at extractor
pp_validation_processed ............ Pipeline preprocessor validated
pp_duplicate ........................ Duplicate at preprocessor
pp_duplicate_skipped ............... Dedup skipped (non-portal)
pp_share_event_processed ........... Share event flattened
denorm_device ...................... Device data enriched
denorm_user ........................ User data enriched
denorm_content ..................... Content data enriched
denorm_collection .................. Collection data enriched
denorm_dialcode .................... DialCode data enriched
denorm_location .................... Location derived
dv_duplicate ....................... Duplicate at validator
```

---

## 📍 File Locations & Structure

```
sunbird-data-pipeline/
├── data-pipeline-flink/
│   ├── dp-core/                          # Shared utilities
│   │   ├── cache/                        # Redis caching
│   │   ├── job/                          # Base classes (BaseJobConfig, BaseProcessFunction)
│   │   ├── util/                         # Utilities (JSONUtil, FlinkUtil)
│   │   └── ...
│   │
│   ├── telemetry-extractor/              # JOB 1
│   │   ├── src/main/scala/org/sunbird/dp/extractor/
│   │   │   ├── task/
│   │   │   │   ├── TelemetryExtractorStreamTask.scala
│   │   │   │   └── TelemetryExtractorConfig.scala
│   │   │   ├── functions/
│   │   │   │   ├── DeduplicationFunction.scala
│   │   │   │   ├── ExtractionFunction.scala
│   │   │   │   └── RedactorFunction.scala
│   │   │   └── domain/
│   │   │       └── Event models
│   │   └── src/main/resources/
│   │       └── application.conf
│   │
│   ├── pipeline-preprocessor/            # JOB 2
│   │   ├── src/main/scala/org/sunbird/dp/preprocessor/
│   │   │   ├── task/
│   │   │   │   ├── PipelinePreprocessorStreamTask.scala
│   │   │   │   └── PipelinePreprocessorConfig.scala
│   │   │   ├── functions/
│   │   │   │   ├── TelemetryValidationFunction.scala
│   │   │   │   ├── TelemetryRouterFunction.scala
│   │   │   │   └── ShareEventsFlattenerFunction.scala
│   │   │   └── util/
│   │   │       └── SchemaValidator.scala
│   │   └── src/main/resources/
│   │       └── application.conf
│   │
│   ├── de-normalization/                 # JOB 3
│   │   ├── src/main/scala/org/sunbird/dp/denorm/
│   │   │   ├── task/
│   │   │   │   ├── DenormalizationStreamTask.scala
│   │   │   │   └── DenormalizationConfig.scala
│   │   │   ├── functions/
│   │   │   │   ├── DenormalizationFunction.scala
│   │   │   │   └── DenormalizationWindowFunction.scala
│   │   │   ├── type/
│   │   │   │   ├── DeviceDenormalization.scala
│   │   │   │   ├── UserDenormalization.scala
│   │   │   │   ├── ContentDenormalization.scala
│   │   │   │   ├── DialcodeDenormalization.scala
│   │   │   │   └── LocationDenormalization.scala
│   │   │   └── util/
│   │   │       └── DenormCache.scala
│   │   └── src/main/resources/
│   │       └── application.conf
│   │
│   ├── druid-events-validator/           # JOB 4
│   │   ├── src/main/scala/org/sunbird/dp/validator/
│   │   │   ├── task/
│   │   │   │   ├── DruidValidatorStreamTask.scala
│   │   │   │   └── DruidValidatorConfig.scala
│   │   │   ├── functions/
│   │   │   │   └── DruidValidatorFunction.scala
│   │   │   └── util/
│   │   │       └── SchemaValidator.scala
│   │   └── src/main/resources/
│   │       └── application.conf
│   │
│   ├── pom.xml                           # Maven parent POM
│   └── ...
│
├── ARCHITECTURE_GUIDE.md                 # This documentation
├── SEQUENCE_DIAGRAM.md
├── PRACTICAL_CODE_GUIDE.md
└── ...
```

---

## 🔍 How to Find Things

### Finding job class
```bash
# Main stream task (entry point)
find . -name "*StreamTask.scala" | grep -i "jobname"

# Configuration class
find . -name "*Config.scala" | grep -i "jobname"

# Processing functions
find . -path "*functions/*.scala" | grep -i "jobname"
```

### Finding configuration
```bash
# Look for application.conf
find . -name "application.conf"

# Check for environment-specific configs
ls -la data-pipeline-flink/*/src/main/resources/
```

### Finding schemas
```bash
# Schema validation files
find . -name "*.json" | grep -i schema

# Or configured path
grep "schema.path" data-pipeline-flink/*/src/main/resources/application.conf
```

---

## 🚀 Quick Start Commands

### Build
```bash
# Build single job
cd data-pipeline-flink/telemetry-extractor
mvn clean package -DskipTests

# Build all jobs
cd data-pipeline-flink
mvn clean package -DskipTests
```

### Run Locally
```bash
# Start local Flink
$FLINK_HOME/bin/start-cluster.sh

# Submit job
$FLINK_HOME/bin/flink run -c org.sunbird.dp.extractor.task.TelemetryExtractorStreamTask \
  data-pipeline-flink/telemetry-extractor/target/*.jar \
  --config /path/to/application.conf

# View UI at: http://localhost:8081
```

### Debug
```bash
# Check Kafka topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Read events
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.raw --from-beginning --max-messages 5

# Check Redis
redis-cli -h localhost -p 6379
DBSIZE
KEYS "*" | head
```

---

## 📈 Performance Tuning

### For Higher Throughput
```properties
# Increase parallelism
task.consumer.parallelism = 8        # Default: 4
task.downstream.operators.parallelism = 16  # Default: 8

# Increase Flink resources
flink.taskmanager.memory.jvm-overhead.fraction = 0.1
flink.taskmanager.memory.process.size = 8g

# Increase Kafka partitions
kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic telemetry.raw --alter --partitions 16
```

### For Lower Latency
```properties
# Reduce batch size in sinks
sink.batch.size = 100  # Default: 1000

# Disable buffering
buffer.flushInterval = 100ms  # Default: 1000ms

# Reduce window size
task.window.count = 10  # Default: 100
```

### Redis Cache Optimization
```properties
# Increase Redis memory
maxmemory = 8gb

# Set eviction policy
maxmemory-policy = allkeys-lru

# Enable persistence for critical data
save 60 1000
```

---

## ⚠️ Common Pitfalls

| Pitfall | Solution |
|---------|----------|
| **High duplicate count** | Reduce Redis TTL or check producer retry config |
| **Jobs failing on Redis error** | Design is fail-fast - fix Redis issue first |
| **Cache misses high** | Update cache updater job, increase Redis memory |
| **Events stuck in failed topic** | Check schema files, verify event structure |
| **Slow denorm job** | Increase parallelism, scale up Redis cluster |
| **Kafka lag increasing** | Increase task parallelism, check downstream processing |

---

## 🧪 Testing Tips

### Unit Testing
```bash
# Run tests for a job
mvn test -pl data-pipeline-flink/telemetry-extractor

# Run specific test
mvn test -pl data-pipeline-flink/telemetry-extractor \
  -Dtest=DeduplicationFunctionTest
```

### Integration Testing
```bash
# Use embedded Kafka & Redis for testing
mvn test -pl data-pipeline-flink/telemetry-extractor \
  -Dtest=TelemetryExtractorStreamTaskTest
```

### Manual Testing
```bash
# 1. Produce test event
kafka-console-producer.sh --broker-list localhost:9092 \
  --topic telemetry.ingest < test-event.json

# 2. Check if processed
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic telemetry.raw --from-beginning

# 3. Check metrics in Flink UI
# http://localhost:8081 → Metrics tab
```

---

## 📚 Additional Resources

### Apache Flink
- [Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [API JavaDoc](https://nightlies.apache.org/flink/flink-docs-stable/api/java/)

### Scala
- [Official Guide](https://docs.scala-lang.org/)
- [Scala Collections](https://docs.scala-lang.org/overviews/collections-2.13/introduction.html)

### Kafka
- [Documentation](https://kafka.apache.org/documentation/)
- [CLI Tools](https://kafka.apache.org/documentation/#cli)

### Redis
- [Commands Reference](https://redis.io/commands/)
- [Cluster Guide](https://redis.io/topics/cluster-tutorial)

### Data Pipeline Specific
- [Druid Documentation](https://druid.apache.org/docs/latest/design/)
- [Secor (Backup Tool)](https://github.com/pinterest/secor)

---

## 🎯 Learning Path

### Week 1: Understanding Architecture
- [ ] Read ARCHITECTURE_GUIDE.md (1 hour)
- [ ] Read SEQUENCE_DIAGRAM.md (1 hour)
- [ ] Review codebase structure (1 hour)
- [ ] Set up local environment (2 hours)

### Week 2: Deep Dive into Jobs
- [ ] Study Telemetry Extractor code (2 hours)
- [ ] Study Pipeline Preprocessor code (2 hours)
- [ ] Study De-normalization code (2 hours)
- [ ] Study Druid Validator code (1 hour)

### Week 3: Hands-On
- [ ] Run jobs locally (2 hours)
- [ ] Trace events through pipeline (2 hours)
- [ ] Modify configuration and observe impact (2 hours)
- [ ] Write simple test case (2 hours)

### Week 4: Advanced Topics
- [ ] Understand error scenarios (2 hours)
- [ ] Performance tuning (2 hours)
- [ ] Production deployment (2 hours)
- [ ] Troubleshooting guide (2 hours)

---

## ✅ Checklist for New Team Members

- [ ] Clone repository and build successfully
- [ ] Read all 3 documentation files
- [ ] Set up local development environment
- [ ] Run telemetry-extractor job locally
- [ ] Trace a test event through entire pipeline
- [ ] Understand configuration for each job
- [ ] Write a simple unit test
- [ ] Debug a job using Flink UI
- [ ] Understand deduplication mechanism
- [ ] Understand denormalization data sources

---

## 📞 Support & Questions

For questions about:
- **Architecture**: See ARCHITECTURE_GUIDE.md
- **Data Flow**: See SEQUENCE_DIAGRAM.md
- **Code & Configuration**: See PRACTICAL_CODE_GUIDE.md
- **Debugging**: See PRACTICAL_CODE_GUIDE.md → Debugging Guide
- **Specific Job**: Search for job name in all documents

---

**Last Updated**: April 27, 2026  
**Version**: 1.0

Happy Learning! 🎉
