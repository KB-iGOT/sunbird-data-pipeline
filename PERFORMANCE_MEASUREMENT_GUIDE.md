# Performance Measurement Guide — Pipeline Optimization Validation

**Purpose:** Prove that the code changes in `cbrelease-4.8.31` deliver measurable throughput, latency, and resource gains before promoting to production.

---

## Table of Contents
1. [Measurement Strategy](#1-measurement-strategy)
2. [Metric Catalog](#2-metric-catalog)
3. [Tools and Access](#3-tools-and-access)
4. [Phase 1 — Baseline Capture (Old Code)](#4-phase-1--baseline-capture-old-code)
5. [Phase 2 — Load Test Procedure](#5-phase-2--load-test-procedure)
6. [Phase 3 — Post-Optimization Measurement (New Code)](#6-phase-3--post-optimization-measurement-new-code)
7. [Comparison Worksheets](#7-comparison-worksheets)
8. [Pass / Fail Criteria](#8-pass--fail-criteria)
9. [Report Template](#9-report-template)

---

## 1. Measurement Strategy

### Approach: Staged Load Test with Metric Snapshots

Do not just deploy to production and observe. That is not comparable because traffic patterns change day to day. Use a **controlled load test** at known TPS levels, capture metrics under each level on both old and new code, then compare like-for-like.

```
OLD CODE                         NEW CODE
─────────────────────────────    ─────────────────────────────
Deploy old branch to staging  →  Deploy new branch to same staging
Run load test at 50k TPS      →  Run load test at 50k TPS
Capture snapshot A            →  Capture snapshot B
Run load test at 100k TPS     →  Run load test at 100k TPS
Capture snapshot C            →  Capture snapshot D
Run load test at 150k TPS     →  Run load test at 150k TPS
Capture snapshot E            →  Capture snapshot F
Find: at what TPS does lag       Find: at what TPS does lag
      start growing?                   start growing?
```

The capacity ceiling is the TPS level at which **any consumer group's lag begins growing**. You want to show that ceiling has moved from ~64k TPS to 100k+ TPS.

### What "Proven Better" Means

You are not looking for marginal improvements. The target outcomes are:

| Metric | Baseline (old code) | Target (new code) |
|---|---|---|
| Capacity ceiling (TPS before lag grows) | ~64k | ≥ 100k |
| Denorm operator throughput | ~15k records/sec/slot | ≥ 60k records/sec/slot |
| Redis ops/sec at 100k TPS | ~800k | ≤ 300k |
| Kafka ops eliminated | 100% | ≥ 60% (telemetry.raw + telemetry.denorm gone) |
| Consumer lag at 100k TPS (sustained 10 min) | Growing | Stable at 0 |

---

## 2. Metric Catalog

All metrics, their source, and the exact query or command to retrieve them.

### Category 1 — Kafka Consumer Lag (Primary Health Indicator)

Lag = how far behind each job is from live traffic. Zero lag = keeping up. Growing lag = capacity breach.

| Metric | What It Tells You | Source |
|---|---|---|
| Lag on `telemetry.ingest` (extractor-group) | Extractor capacity | Prometheus |
| Lag on `telemetry.raw` (preprocessor-group) | Preprocessor capacity — should drop to 0 when telemetry-intake replaces both | Prometheus |
| Lag on `telemetry.unique.primary` (denorm-primary-group) | Denorm primary capacity | Prometheus |
| Lag on `telemetry.unique.secondary` (denorm-secondary-group) | Denorm secondary capacity | Prometheus |
| Lag on `telemetry.denorm` (druid-validator-group) | Druid validator capacity — should drop to 0 after P3.1 | Prometheus |

**Prometheus queries (replace `{ENV}` with your environment name, e.g. `prod`):**

```promql
# Extractor / telemetry-intake consumer lag
sum(kafka_consumergroup_lag_sum{
  consumergroup="{ENV}-telemetry-extractor-group",
  topic="{ENV}.telemetry.ingest"
})

# Pipeline-preprocessor lag (old code)
sum(kafka_consumergroup_lag_sum{
  consumergroup="{ENV}-pipeline-preprocessor-group",
  topic="{ENV}.telemetry.raw"
})

# De-normalization primary lag
sum(kafka_consumergroup_lag_sum{
  consumergroup="{ENV}-telemetry-denorm-primary-group",
  topic="{ENV}.telemetry.unique.primary"
})

# De-normalization secondary lag
sum(kafka_consumergroup_lag_sum{
  consumergroup="{ENV}-telemetry-denorm-secondary-group",
  topic="{ENV}.telemetry.unique.secondary"
})

# Druid validator lag (old code)
sum(kafka_consumergroup_lag_sum{
  consumergroup="{ENV}-druid-validator-group",
  topic="{ENV}.telemetry.denorm"
})

# All consumer group lags at once — useful for a single dashboard panel
sum by (consumergroup) (kafka_consumergroup_lag_sum{
  consumergroup=~"{ENV}-(telemetry-extractor|pipeline-preprocessor|telemetry-denorm-primary|telemetry-denorm-secondary|druid-validator)-group"
})
```

**CLI alternative (no Prometheus needed):**
```bash
# Run for each consumer group
BOOTSTRAP="<broker-host>:9092"
ENV="prod"

for GROUP in \
  "${ENV}-telemetry-extractor-group" \
  "${ENV}-pipeline-preprocessor-group" \
  "${ENV}-telemetry-denorm-primary-group" \
  "${ENV}-telemetry-denorm-secondary-group" \
  "${ENV}-druid-validator-group"; do
  echo "===== $GROUP ====="
  kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
    --describe --group $GROUP 2>/dev/null | \
    awk 'NR==1 || /TOPIC/ {print} NR>1 && !/TOPIC/ {lag+=$6} END {print "TOTAL LAG:", lag}'
done
```

---

### Category 2 — Kafka Topic Throughput (Events/Sec)

Measures the actual event rate flowing through each topic.

**Prometheus queries:**
```promql
# Produce rate to telemetry.ingest (what the SDK is sending)
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic="{ENV}.telemetry.ingest"}[1m]))

# Produce rate to telemetry.raw (old code: extractor output; new code: shadow write)
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic="{ENV}.telemetry.raw"}[1m]))

# Produce rate to telemetry.unique (de-norm primary input)
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic="{ENV}.telemetry.unique"}[1m]))

# Produce rate to druid.events.telemetry (final Druid output)
sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_count{topic="{ENV}.druid.events.telemetry"}[1m]))
```

**CLI alternative:**
```bash
# Monitor a topic's messages/sec for 30 seconds
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic ${ENV}.telemetry.ingest

# Offset change = events/sec (run twice, 10 seconds apart, subtract)
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server $BOOTSTRAP \
  --topic ${ENV}.telemetry.ingest \
  --time -1 | awk -F: '{sum+=$3} END {print "Total offset:", sum}'
```

---

### Category 3 — Flink Operator Throughput and Back Pressure

Measures how fast each Flink operator processes records and whether it is back-pressuring upstream.

**Prometheus metric names** (Flink PrometheusReporter, scraped from task manager pods):

```promql
# Records processed per second — denorm operator (key metric: does new async code process faster?)
flink_taskmanager_job_task_numRecordsInPerSecond{
  job="{ENV}-de-normalization-taskmanager-prometheus"
}

# Records processed per second — telemetry-intake (new) vs extractor+preprocessor (old)
flink_taskmanager_job_task_numRecordsInPerSecond{
  job=~"{ENV}-(telemetry-intake|telemetry-extractor|pipeline-preprocessor)-taskmanager-prometheus"
}

# Back pressure: milliseconds per second the operator is back-pressured (0 = healthy, 1000 = fully blocked)
flink_taskmanager_job_task_backPressuredTimeMsPerSecond{
  job="{ENV}-de-normalization-taskmanager-prometheus"
}

# Checkpoint duration (longer = heavier state)
flink_jobmanager_job_lastCheckpointDuration{
  job="{ENV}-de-normalization-taskmanager-prometheus"
}

# Kafka consumer lag from Flink's own metrics (per operator)
sum(flink_taskmanager_job_task_operator_KafkaConsumer_records_lag_max{
  job="{ENV}-de-normalization-primary-taskmanager-prometheus"
})
```

**Via Flink REST API** (available at the Flink Web UI — `http://<flink-jm-service>:8081`):

```bash
# Get all running jobs
FLINK_JM="http://<jobmanager-service>:8081"
curl -s "${FLINK_JM}/jobs" | python3 -m json.tool

# Get job ID for de-normalization
JOB_ID=$(curl -s "${FLINK_JM}/jobs" | python3 -c "
import json,sys
jobs = json.load(sys.stdin)['jobs']
[print(j['id']) for j in jobs if j['status']=='RUNNING']
" | head -1)

# Get operator-level metrics
curl -s "${FLINK_JM}/jobs/${JOB_ID}/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,backPressuredTimeMsPerSecond" | python3 -m json.tool

# Get per-subtask throughput
curl -s "${FLINK_JM}/jobs/${JOB_ID}/vertices" | python3 -m json.tool
```

---

### Category 4 — Flink Application Metrics (Custom Gauges)

These are the per-enrichment-type hit/miss counters registered in `DenormalizationAsyncFunction` and the preprocessor functions. They expose:
- **Caffeine cache hit rate** — how often we skip Redis (new code only)
- **Enrichment skip rate** — how often LOG/ERROR/AUDIT skip Redis entirely (new code only)
- **Dedup skip rate** — how often `isDuplicateCheckRequired` skips Redis (fixed in P1.3)

**Prometheus metric names:**

The custom gauges are registered as:
`flink_taskmanager_job_task_operator_{jobName}_{metricName}`

where `jobName` is the Flink job name (e.g. `DenormalizationJob`) and `metricName` is the gauge name.

```promql
# Caffeine device cache hit rate (new code only)
# Hit rate = cache-hit / (cache-hit + cache-miss) — should be 50-70%
sum(flink_taskmanager_job_task_operator_DenormalizationJob_device_cache_hit)
/
(sum(flink_taskmanager_job_task_operator_DenormalizationJob_device_cache_hit) +
 sum(flink_taskmanager_job_task_operator_DenormalizationJob_device_cache_miss))

# User cache hit rate
sum(flink_taskmanager_job_task_operator_DenormalizationJob_user_cache_hit)
/
(sum(flink_taskmanager_job_task_operator_DenormalizationJob_user_cache_hit) +
 sum(flink_taskmanager_job_task_operator_DenormalizationJob_user_cache_miss))

# Content cache hit rate
sum(flink_taskmanager_job_task_operator_DenormalizationJob_content_cache_hit)
/
(sum(flink_taskmanager_job_task_operator_DenormalizationJob_content_cache_hit) +
 sum(flink_taskmanager_job_task_operator_DenormalizationJob_content_cache_miss))

# Events skipped (LOG/ERROR/AUDIT bypass Redis — P2.4)
sum(rate(flink_taskmanager_job_task_operator_DenormalizationJob_events_skipped[1m]))

# Events expired (too old, dropped)
sum(rate(flink_taskmanager_job_task_operator_DenormalizationJob_events_expired[1m]))

# Preprocessor dedup skip (fixed in P1.3 — non-portal events skip Redis dedup)
sum(rate(flink_taskmanager_job_task_operator_PipelinePreprocessorJob_pp_duplicate_skipped[1m]))
```

**Note on metric name format:** The exact Prometheus label path depends on your Flink version and how the MetricGroup scope is configured. If the above queries return nothing, check the actual metric names by running:
```bash
# Scrape the task manager Prometheus endpoint directly
kubectl exec -n <flink-namespace> <taskmanager-pod> -- \
  curl -s localhost:9251/metrics | grep -i "denorm\|cache\|skip"
```

---

### Category 5 — Redis Metrics

Measures how hard the pipeline is hitting Redis.

**Per-Redis-instance commands** (run on each of the 4 Redis instances: device, user, content, dialcode):

```bash
# Connect to Redis (replace host/port per instance)
REDIS_HOST="<redis-host>"
REDIS_PORT="6379"

# 1. Instantaneous ops/sec — the headline number
redis-cli -h $REDIS_HOST -p $REDIS_PORT INFO stats | grep instantaneous_ops_per_sec

# 2. Cache hit rate = keyspace_hits / (keyspace_hits + keyspace_misses)
redis-cli -h $REDIS_HOST -p $REDIS_PORT INFO stats | grep -E "keyspace_hits|keyspace_misses"

# 3. Connected clients (high count = connection pool working)
redis-cli -h $REDIS_HOST -p $REDIS_PORT INFO clients | grep connected_clients

# 4. Command latency distribution (requires latency monitoring enabled)
redis-cli -h $REDIS_HOST -p $REDIS_PORT LATENCY LATEST

# 5. Slowlog (commands that took > configured threshold)
redis-cli -h $REDIS_HOST -p $REDIS_PORT SLOWLOG GET 10

# 6. Memory usage
redis-cli -h $REDIS_HOST -p $REDIS_PORT INFO memory | grep -E "used_memory_human|maxmemory_human"

# Full stats snapshot (save to file)
redis-cli -h $REDIS_HOST -p $REDIS_PORT INFO ALL > redis_stats_$(date +%Y%m%d_%H%M%S).txt
```

**Run this script to capture all 4 Redis instances at once:**
```bash
#!/bin/bash
# save as capture_redis.sh
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
declare -A INSTANCES=(
  ["device"]="<device-redis-host>:6381"
  ["user"]="<user-redis-host>:6382"
  ["content"]="<content-redis-host>:6380"
  ["dialcode"]="<dialcode-redis-host>:6383"
  ["dedup"]="<dedup-redis-host>:6379"
)

for NAME in "${!INSTANCES[@]}"; do
  IFS=':' read -r HOST PORT <<< "${INSTANCES[$NAME]}"
  echo "=== $NAME ($HOST:$PORT) ===" | tee -a redis_snapshot_${TIMESTAMP}.txt
  redis-cli -h $HOST -p $PORT INFO stats 2>/dev/null | \
    grep -E "instantaneous_ops_per_sec|keyspace_hits|keyspace_misses|total_commands_processed" | \
    tee -a redis_snapshot_${TIMESTAMP}.txt
  redis-cli -h $HOST -p $PORT INFO clients 2>/dev/null | \
    grep connected_clients | tee -a redis_snapshot_${TIMESTAMP}.txt
  echo "" | tee -a redis_snapshot_${TIMESTAMP}.txt
done
echo "Saved to redis_snapshot_${TIMESTAMP}.txt"
```

---

### Category 6 — JVM and Pod Resource Metrics

Measures per-pod CPU, memory, and GC to confirm the optimization didn't just shift load elsewhere.

**Kubernetes resource metrics:**
```bash
# CPU and memory per pod (requires metrics-server)
kubectl top pods -n <flink-namespace> --sort-by=cpu

# Watch continuously
watch -n 5 kubectl top pods -n <flink-namespace> --sort-by=cpu
```

**Prometheus queries:**
```promql
# CPU usage per Flink pod
sum by (pod) (rate(container_cpu_usage_seconds_total{
  namespace="<flink-namespace>",
  container="flink-main-container"
}[2m]))

# Memory usage per Flink pod
sum by (pod) (container_memory_working_set_bytes{
  namespace="<flink-namespace>",
  container="flink-main-container"
})

# JVM GC pause time (old Flink JVM metrics)
sum(flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Time)
sum(flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time)

# JVM heap usage
flink_taskmanager_Status_JVM_Memory_Heap_Used /
flink_taskmanager_Status_JVM_Memory_Heap_Max
```

---

### Category 7 — End-to-End Pipeline Latency

The time from when an event is ingested at `telemetry.ingest` to when it appears in Druid.

**Method 1 — Kafka timestamp comparison (easiest)**
```bash
# Read from beginning of telemetry.ingest and note the Kafka message timestamp
kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic ${ENV}.telemetry.ingest \
  --max-messages 1 \
  --property print.timestamp=true \
  --from-beginning 2>/dev/null | head -5

# Read matching event from druid.events.telemetry and compare Kafka timestamps
kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic ${ENV}.druid.events.telemetry \
  --max-messages 100 \
  --property print.timestamp=true 2>/dev/null | \
  python3 -c "
import sys
for line in sys.stdin:
    parts = line.split('\t')
    if len(parts) >= 2:
        print(f'Kafka timestamp: {parts[0]}, event mid: ...check edata...')
"
```

**Method 2 — Inject a test event and track it (most accurate)**
```bash
# Step 1: Create a test batch event with a known mid
TEST_MID="perf-test-$(date +%s%N)"
TEST_EVENT=$(cat <<EOF
{
  "id": "sunbird.telemetry",
  "ver": "3.0",
  "ets": $(date +%s%3N),
  "params": {"msgid": "${TEST_MID}"},
  "events": [{
    "eid": "START",
    "ets": $(date +%s%3N),
    "ver": "3.0",
    "mid": "${TEST_MID}-evt",
    "actor": {"id": "perf-test-user", "type": "User"},
    "context": {"channel": "perf-test", "pdata": {"id": "perf.test.portal", "ver": "1.0"}},
    "edata": {"type": "perf-test"}
  }]
}
EOF
)

# Step 2: Produce to telemetry.ingest and note the wall-clock time
PRODUCE_TIME=$(date +%s%3N)
echo "${TEST_EVENT}" | kafka-console-producer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic ${ENV}.telemetry.ingest

# Step 3: Wait and grep druid.events.telemetry for the mid
TIMEOUT=120  # seconds
START=$(date +%s)
while [ $(($(date +%s) - START)) -lt $TIMEOUT ]; do
  FOUND=$(kafka-console-consumer.sh \
    --bootstrap-server $BOOTSTRAP \
    --topic ${ENV}.druid.events.telemetry \
    --max-messages 1000 \
    --timeout-ms 5000 2>/dev/null | grep "${TEST_MID}")
  if [ -n "$FOUND" ]; then
    ARRIVE_TIME=$(date +%s%3N)
    LATENCY=$((ARRIVE_TIME - PRODUCE_TIME))
    echo "Event found! End-to-end latency: ${LATENCY}ms"
    break
  fi
  sleep 1
done
```

**Method 3 — Druid query for recent events (if Druid API is accessible)**
```bash
# Query Druid for events in the last 5 minutes and check freshness
curl -X POST \
  "http://<druid-router>:8888/druid/v2/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT COUNT(*) as cnt, MAX(__time) as latest_event FROM telemetry_events WHERE __time > CURRENT_TIMESTAMP - INTERVAL '\''5'\'' MINUTE",
    "resultFormat": "object"
  }' | python3 -m json.tool
```

---

## 3. Tools and Access

### Tool Checklist

| Tool | How to Access | What It Measures |
|---|---|---|
| Prometheus UI | `http://<prometheus-svc>:9090` | All time-series metrics |
| Grafana | `http://<grafana-svc>:3000` | Dashboards (if configured) |
| Flink Web UI | `http://<jobmanager-svc>:8081` | Job topology, per-operator throughput, back pressure |
| `kafka-consumer-groups.sh` | In any Kafka pod: `kubectl exec -n <ns> <kafka-pod> -- kafka-consumer-groups.sh` | Consumer lag per partition |
| `redis-cli` | In any pod with Redis access, or port-forward: `kubectl port-forward svc/<redis-svc> 6379:6379 -n <ns>` | Redis ops/sec, hit rate |
| `kubectl top` | `kubectl top pods -n <flink-namespace>` | CPU and memory per pod |
| Flink REST API | `curl http://<jobmanager-svc>:8081/jobs` | Job and operator metrics programmatically |

### Port-Forward Shortcuts for Local Access
```bash
# Prometheus
kubectl port-forward svc/<prometheus-svc> 9090:9090 -n monitoring &

# Grafana
kubectl port-forward svc/<grafana-svc> 3000:3000 -n monitoring &

# Flink Web UI (de-normalization job)
kubectl port-forward svc/de-normalization-rest 8081:8081 -n <flink-namespace> &

# Redis (device store, for example)
kubectl port-forward svc/<device-redis-svc> 6381:6379 -n <flink-namespace> &
```

---

## 4. Phase 1 — Baseline Capture (Old Code)

Run this on the **old branch** (before any optimization changes are deployed to the test environment).

### Step 1 — Document Environment State
```bash
# Record cluster state
echo "=== Capture Date ===" && date
echo "=== Branch ===" && git rev-parse --abbrev-ref HEAD && git rev-parse HEAD
echo "=== Parallelism ===" && grep "parallelism" data-pipeline-flink/dp-core/src/main/resources/base-config.conf
echo "=== Job Versions ===" && kubectl get pods -n <flink-namespace> -o wide | grep -v Terminating

# Save to file
{
  echo "Baseline capture: $(date)"
  echo "Git: $(git rev-parse --abbrev-ref HEAD) @ $(git rev-parse HEAD)"
  kubectl get pods -n <flink-namespace> --no-headers | awk '{print $1, $3}'
} > baseline_environment.txt
```

### Step 2 — Capture Zero-Load Baseline (Idle State)
```bash
# With no load, capture Redis idle ops
bash capture_redis.sh > baseline_redis_idle.txt

# Kafka lag at idle (should be 0)
for GROUP in \
  "{ENV}-telemetry-extractor-group" \
  "{ENV}-pipeline-preprocessor-group" \
  "{ENV}-telemetry-denorm-primary-group" \
  "{ENV}-telemetry-denorm-secondary-group" \
  "{ENV}-druid-validator-group"; do
  echo "=== $GROUP ===" >> baseline_kafka_idle.txt
  kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
    --describe --group $GROUP 2>/dev/null >> baseline_kafka_idle.txt
done

# Pod resources at idle
kubectl top pods -n <flink-namespace> > baseline_pod_resources_idle.txt
```

### Step 3 — Capture Under Live Traffic (If Production Staging)

If the staging environment has real traffic flowing through it, capture a 5-minute sample at each of 3 different times (morning, afternoon, evening) to get a representative baseline:

```bash
# Capture every metric category at t=0, t+5min, t+10min
for i in 1 2 3; do
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  echo "Snapshot $i at $TIMESTAMP"
  
  # Kafka consumer group lag
  for GROUP in "{ENV}-telemetry-extractor-group" "{ENV}-pipeline-preprocessor-group" \
               "{ENV}-telemetry-denorm-primary-group" "{ENV}-druid-validator-group"; do
    echo "=== $GROUP ===" >> baseline_lag_snapshot_${i}.txt
    kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
      --describe --group "$GROUP" 2>/dev/null | \
      awk 'NR>1 {lag+=$6} END {print "TOTAL LAG:", lag}' >> baseline_lag_snapshot_${i}.txt
  done
  
  # Redis ops
  bash capture_redis.sh >> baseline_redis_live_${i}.txt
  
  # Pod CPU/memory
  kubectl top pods -n <flink-namespace> >> baseline_pod_resources_${i}.txt
  
  sleep 300  # 5 minutes between snapshots
done
```

### Step 4 — Record Baseline Prometheus Metrics

In Prometheus UI, execute each query from Section 2 and note the values. Use the **"Table" view** for point-in-time values and **"Graph" view** with a 30-minute window for trend analysis.

Save screenshots of:
- Consumer lag over time (all consumer groups on one graph)
- `numRecordsInPerSecond` for de-normalization operators
- `backPressuredTimeMsPerSecond` for de-normalization operators
- Redis `instantaneous_ops_per_sec` for all 4 instances

---

## 5. Phase 2 — Load Test Procedure

### Load Generator Setup

You need to produce synthetic telemetry batch events to `telemetry.ingest` at a controlled rate.

**Step 1 — Create the load generator script:**

```python
#!/usr/bin/env python3
# save as: load_generator.py
# requires: pip install kafka-python

import json
import time
import uuid
import random
import argparse
from kafka import KafkaProducer

def make_batch(events_per_batch=10):
    """Mimics a real SDK batch event."""
    now_ms = int(time.time() * 1000)
    events = []
    for _ in range(events_per_batch):
        eid = random.choice(["INTERACT", "IMPRESSION", "START", "END", "SEARCH", "LOG", "AUDIT"])
        events.append({
            "eid": eid,
            "ets": now_ms,
            "ver": "3.0",
            "mid": str(uuid.uuid4()),
            "actor": {"id": f"user-{random.randint(1,100000)}", "type": "User"},
            "context": {
                "channel": "test-channel",
                "pdata": {"id": "perf.test.portal", "ver": "1.0"},
                "did": f"device-{random.randint(1,10000)}"
            },
            "object": {"id": f"content-{random.randint(1,5000)}", "type": "Content"},
            "edata": {"type": "perf-test", "pageid": "home"}
        })
    return {
        "id": "sunbird.telemetry",
        "ver": "3.0",
        "ets": now_ms,
        "params": {"msgid": str(uuid.uuid4())},
        "events": events
    }

def run(broker, topic, target_tps, duration_seconds, events_per_batch=10):
    """
    target_tps: individual events/sec (not batches/sec)
    events_per_batch: how many events per Kafka message (default 10, mimics real SDK)
    """
    batches_per_sec = target_tps / events_per_batch
    sleep_between_batches = 1.0 / batches_per_sec

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=262144,
        linger_ms=20,
        compression_type='snappy'
    )

    start = time.time()
    sent_batches = 0
    sent_events = 0

    print(f"Starting load: target {target_tps} events/sec, {batches_per_sec:.1f} batches/sec")
    print(f"Duration: {duration_seconds}s | Broker: {broker} | Topic: {topic}")

    while time.time() - start < duration_seconds:
        batch = make_batch(events_per_batch)
        producer.send(topic, batch)
        sent_batches += 1
        sent_events += events_per_batch

        elapsed = time.time() - start
        actual_tps = sent_events / elapsed if elapsed > 0 else 0

        if sent_batches % 100 == 0:
            print(f"t={elapsed:.0f}s | sent={sent_events} events | actual TPS={actual_tps:.0f}")

        time.sleep(sleep_between_batches)

    producer.flush()
    elapsed = time.time() - start
    print(f"\nDone. Sent {sent_events} events in {elapsed:.1f}s = {sent_events/elapsed:.0f} TPS average")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", required=True, help="Kafka broker, e.g. localhost:9092")
    parser.add_argument("--topic", required=True, help="Topic to produce to, e.g. prod.telemetry.ingest")
    parser.add_argument("--tps", type=int, required=True, help="Target events/sec")
    parser.add_argument("--duration", type=int, default=600, help="Duration in seconds (default 600)")
    parser.add_argument("--batch-size", type=int, default=10, help="Events per Kafka message (default 10)")
    args = parser.parse_args()
    run(args.broker, args.topic, args.tps, args.duration, args.batch_size)
```

**Step 2 — Load test ramp plan:**

| Test Run | TPS | Duration | Purpose |
|---|---|---|---|
| T1 | 30,000 | 10 min | Warm-up, verify no errors |
| T2 | 50,000 | 10 min | Below old ceiling — both old and new should handle this |
| T3 | 64,000 | 10 min | Old ceiling — old code should start struggling here |
| T4 | 100,000 | 10 min | New target — new code should handle this cleanly |
| T5 | 130,000 | 10 min | Stress test — find new code ceiling |
| T6 | 150,000 | 10 min | Maximum stress |

**Step 3 — Run each test level:**

```bash
# Example: Run T4 (100k TPS for 10 minutes)
python3 load_generator.py \
  --broker "<broker-host>:9092" \
  --topic "prod.telemetry.ingest" \
  --tps 100000 \
  --duration 600 \
  --batch-size 10

# While it runs (in another terminal), capture metrics every 60 seconds:
for i in $(seq 1 10); do
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  echo "=== T=+${i}min at $TIMESTAMP ===" >> loadtest_100k_metrics.txt
  
  # Kafka lag
  for GROUP in "{ENV}-telemetry-extractor-group" "{ENV}-telemetry-denorm-primary-group"; do
    echo "--- $GROUP ---" >> loadtest_100k_metrics.txt
    kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
      --describe --group "$GROUP" 2>/dev/null | \
      awk 'NR>1 {lag+=$6} END {print "LAG:", lag}' >> loadtest_100k_metrics.txt
  done
  
  # Redis ops
  redis-cli -h <device-redis-host> -p 6381 INFO stats 2>/dev/null | \
    grep instantaneous_ops_per_sec >> loadtest_100k_metrics.txt
    
  sleep 60
done
```

### Step 4 — The Critical Observation

During each test run, watch the Kafka consumer group lag trend:
- **Lag is flat at 0** = job is keeping up. ✅
- **Lag is growing** = job cannot keep up. ❌ Record the TPS at which this happens.

```bash
# Watch lag in real-time (run while load test is running)
watch -n 10 'for GROUP in \
  "{ENV}-telemetry-extractor-group" \
  "{ENV}-pipeline-preprocessor-group" \
  "{ENV}-telemetry-denorm-primary-group" \
  "{ENV}-druid-validator-group"; do
    echo "--- $GROUP ---"
    kafka-consumer-groups.sh --bootstrap-server '"$BOOTSTRAP"' \
      --describe --group "$GROUP" 2>/dev/null | \
      awk "NR>1 {lag+=\$6} END {print \"LAG:\", lag}"
done'
```

---

## 6. Phase 3 — Post-Optimization Measurement (New Code)

Deploy the `cbrelease-4.8.31` branch to the same staging environment, then repeat the exact same measurement procedure from Phase 1 and Phase 2.

### Critical Differences in New Code to Verify

Before starting the load test on new code, confirm these specific behaviors are working:

**1. Caffeine cache is populating (new code only):**
```bash
# After 5 minutes of traffic, hit rate should be > 30%
kubectl exec -n <flink-namespace> <denorm-taskmanager-pod> -- \
  curl -s localhost:9251/metrics | grep -E "device.cache.hit|device.cache.miss"
# Expected: hit > miss after warm-up period
```

**2. `isDuplicateCheckRequired` fix is active (P1.3):**
```bash
# The pp_duplicate_skipped counter should be NON-ZERO after events flow
kubectl exec -n <flink-namespace> <preprocessor-taskmanager-pod> -- \
  curl -s localhost:9251/metrics | grep "duplicate_skipped"
# Expected: a positive, growing number
```

**3. Enrichment skip is active for LOG/AUDIT events (P2.4):**
```bash
# events-skipped gauge should increase as LOG/AUDIT events flow
kubectl exec -n <flink-namespace> <denorm-taskmanager-pod> -- \
  curl -s localhost:9251/metrics | grep "events_skipped"
```

**4. Bloom filter is reducing dedup Redis calls:**

The bloom filter effect is indirect — you observe it via lower Redis ops/sec on the dedup Redis instances (DB1 and DB2) compared to baseline, at the same TPS.

```bash
# Dedup Redis (DB1 for extractor, DB2 for preprocessor)
redis-cli -h <dedup-redis-host> -p 6379 INFO stats | grep instantaneous_ops_per_sec
# Expected: significantly lower than baseline at same TPS
```

**5. Async denorm is not back-pressuring:**
```promql
# Should be near 0 (no back pressure)
flink_taskmanager_job_task_backPressuredTimeMsPerSecond{
  job=~".*de-normalization.*"
}
```

---

## 7. Comparison Worksheets

Copy these tables, fill in actual numbers from your measurements, and include in your review document.

### Worksheet 1 — Capacity Ceiling (Most Important)

| TPS Level | Old Code — Lag Growing? | New Code — Lag Growing? |
|---|---|---|
| 30,000 | — | — |
| 50,000 | — | — |
| 64,000 | — | — |
| 100,000 | — | — |
| 130,000 | — | — |
| 150,000 | — | — |
| **Ceiling TPS** | **~64,000** | **Target: ≥100,000** |

**How to fill in:** Write "Stable" if lag stayed at 0 throughout the 10-minute run. Write "Growing (peaked at X)" if lag grew.

---

### Worksheet 2 — Consumer Lag at Sustained 100k TPS

Capture lag values at 2-minute intervals during a 10-minute 100k TPS run.

| Time (min) | Extractor/Intake lag | Denorm-Primary lag | Denorm-Secondary lag | Druid-Validator lag |
|---|---|---|---|---|
| t+2 | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: |
| t+4 | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: |
| t+6 | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: |
| t+8 | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: |
| t+10 | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: | OLD: / NEW: |
| **Trend** | Flat=✅ Growing=❌ | Flat=✅ Growing=❌ | Flat=✅ Growing=❌ | N/A (job removed) |

---

### Worksheet 3 — Kafka Topic I/O

Measured at sustained 100k TPS input to `telemetry.ingest`.

| Topic | Old Code — Produce Rate (events/sec) | New Code — Produce Rate (events/sec) | Change |
|---|---|---|---|
| `{ENV}.telemetry.ingest` | | | (should be same — input) |
| `{ENV}.telemetry.raw` | | Shadow write only | **Eliminated from hot path** |
| `{ENV}.telemetry.unique` | | | (should be same) |
| `{ENV}.telemetry.denorm` | | Shadow write only | **Eliminated from hot path** |
| `{ENV}.druid.events.telemetry` | | | (should be same — output) |
| **Total Kafka ops/sec** | | | **Target: ≥50% reduction** |

---

### Worksheet 4 — Denorm Operator Throughput

| Metric | Old Code | New Code | Change |
|---|---|---|---|
| `numRecordsInPerSecond` (per slot) | | | Target: ≥3× |
| `backPressuredTimeMsPerSecond` | | | Target: ≤50 (was ~900) |
| Last checkpoint duration (ms) | | | |
| Checkpoint size (MB) | | | |

---

### Worksheet 5 — Redis Operations Per Second

Measured at sustained 100k TPS. Capture from all Redis instances combined.

| Redis Instance | Old Code — ops/sec | New Code — ops/sec | Reduction |
|---|---|---|---|
| Device (port 6381) | | | |
| User (port 6382) | | | |
| Content (port 6380) | | | |
| Dialcode (port 6383) | | | |
| Dedup (port 6379) | | | |
| **Total Redis ops/sec** | | | **Target: ≥50% reduction** |

---

### Worksheet 6 — Caffeine Cache Hit Rates (New Code Only)

Measured after 10 minutes of 100k TPS (after warm-up).

| Cache | Hits/sec | Misses/sec | Hit Rate | Redis calls saved/sec |
|---|---|---|---|---|
| Device Caffeine cache | | | | |
| User Caffeine cache | | | | |
| Content Caffeine cache | | | | |
| Dialcode Caffeine cache | | | | |

---

### Worksheet 7 — Dedup Optimization (P1.3 + P2.1)

| Metric | Old Code | New Code | Note |
|---|---|---|---|
| `pp_duplicate_skipped` rate/sec | 0 (bug) | > 0 | P1.3 fix |
| `pp_duplicate` (actual duplicates caught) rate/sec | | | |
| Dedup Redis ops/sec | | | Bloom + P1.3 combined |

---

### Worksheet 8 — Pod Resource Efficiency

| Pod / Job | Old Code CPU | New Code CPU | Old Code Memory | New Code Memory |
|---|---|---|---|---|
| telemetry-extractor | | N/A (job removed) | | |
| pipeline-preprocessor | | N/A (job removed) | | |
| telemetry-intake | N/A | | N/A | |
| de-normalization-primary | | | | |
| druid-events-validator | | N/A (job removed) | | |
| **Total pod count (hot path)** | **4 jobs** | **2 jobs** | | |

---

### Worksheet 9 — End-to-End Latency

Use the test event injection method from Section 2, Category 7. Run 10 injections, record each latency, report median and p95.

| Test # | Produce Timestamp (ms) | Arrive at druid.events.telemetry (ms) | Latency (ms) |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |
| 5 | | | |
| 6 | | | |
| 7 | | | |
| 8 | | | |
| 9 | | | |
| 10 | | | |
| **Median** | | | Target: ≤30% reduction |
| **p95** | | | |

---

## 8. Pass / Fail Criteria

A result is **PASS** if all of the following are met. If any single item fails, do not promote to production.

| # | Test | Pass Condition | How to Verify |
|---|---|---|---|
| 1 | **Capacity ceiling raised** | Consumer lag stays flat at 0 for full 10-min run at 100k TPS | Worksheet 2 — all lags flat |
| 2 | **No lag regression at 50k TPS** | New code handles 50k TPS with same or less lag than old code | Worksheet 2 at 50k |
| 3 | **Denorm throughput improved** | `numRecordsInPerSecond` per slot ≥ 3× baseline | Worksheet 4 |
| 4 | **No back pressure at 100k TPS** | `backPressuredTimeMsPerSecond` ≤ 50ms at 100k TPS (was ~900ms) | Worksheet 4 |
| 5 | **Redis ops reduced** | Total Redis ops/sec ≤ 50% of baseline at same TPS | Worksheet 5 |
| 6 | **Caffeine cache working** | Device + Content cache hit rate ≥ 40% after 10-min warm-up | Worksheet 6 |
| 7 | **P1.3 dedup fix active** | `pp_duplicate_skipped` rate > 0 | Worksheet 7 |
| 8 | **Kafka I/O reduced** | `telemetry.raw` produce rate = 0 for telemetry-intake job consumers **OR** clearly labeled as shadow only | Worksheet 3 |
| 9 | **No data loss** | `druid.events.telemetry` produce rate matches `telemetry.ingest` minus expected LOG/ERROR events (±2%) | Worksheet 3 — compare ingest vs druid output |
| 10 | **No job restarts** | Zero job failures/restarts during all load test runs | `kubectl get pods -n <ns>` — no CrashLoopBackOff, no Flink job failure in Web UI |

---

## 9. Report Template

Use this structure when presenting results for review.

```
# Pipeline Optimization Performance Validation Report

**Test Date:** YYYY-MM-DD
**Environment:** staging / pre-prod
**Git SHA (old):** <sha>
**Git SHA (new):** <sha>
**Tester:** <name>

## Executive Summary
[2-3 sentences: did it pass, what was the capacity improvement, what was reduced]

## Key Results

| Metric | Baseline | After Optimization | Improvement |
|---|---|---|---|
| Capacity ceiling (TPS) | 64,000 | [actual] | [actual %] |
| Consumer lag at 100k TPS | Growing | [stable/growing] | [pass/fail] |
| Denorm records/sec/slot | [actual] | [actual] | [actual ×] |
| Total Redis ops/sec at 100k TPS | [actual] | [actual] | [actual % reduction] |
| Total Kafka ops/sec | [actual] | [actual] | [actual % reduction] |
| End-to-end latency (median) | [actual ms] | [actual ms] | [actual % reduction] |
| Hot-path jobs count | 4 | 2 | −2 jobs |

## Capacity Ceiling Test
[Include Worksheet 1 filled in]
[Include screenshot of Grafana/Prometheus lag graph at 100k TPS — old code vs new code side by side]

## Denorm Throughput
[Include Worksheet 4]
[Include screenshot of numRecordsInPerSecond before vs after]

## Redis Pressure
[Include Worksheet 5 and 6]
[Include screenshot of Redis INFO stats before vs after]

## Risks and Mitigations
- telemetry.raw shadow write adds ~X% overhead → mitigated: removed after 30-day drain
- [any other findings from load test]

## Recommendation
[ ] PASS — promote to production
[ ] CONDITIONAL PASS — promote with following caveats: ...
[ ] FAIL — do not promote, issues: ...
```

---

### Quick-Start: Capture Everything in 5 Minutes

If you just want to quickly capture the current state without the full load test procedure:

```bash
#!/bin/bash
# save as quick_capture.sh
# Run on any pod with kafka + redis access

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BOOTSTRAP="<broker>:9092"
OUTPUT="pipeline_snapshot_${TIMESTAMP}"
mkdir -p $OUTPUT

echo "Capturing pipeline snapshot at $TIMESTAMP..."

# 1. Kafka consumer lag
echo "→ Kafka consumer lag..."
for GROUP in \
  "{ENV}-telemetry-extractor-group" \
  "{ENV}-pipeline-preprocessor-group" \
  "{ENV}-telemetry-denorm-primary-group" \
  "{ENV}-telemetry-denorm-secondary-group" \
  "{ENV}-druid-validator-group"; do
  echo "=== $GROUP ===" >> $OUTPUT/kafka_lag.txt
  kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
    --describe --group "$GROUP" 2>/dev/null >> $OUTPUT/kafka_lag.txt
done

# 2. Redis ops (adjust hosts/ports)
echo "→ Redis stats..."
for SPEC in "device:6381" "user:6382" "content:6380" "dialcode:6383" "dedup:6379"; do
  NAME="${SPEC%%:*}"
  PORT="${SPEC##*:}"
  echo "=== $NAME ===" >> $OUTPUT/redis_stats.txt
  redis-cli -h localhost -p $PORT INFO stats 2>/dev/null | \
    grep -E "instantaneous_ops|keyspace" >> $OUTPUT/redis_stats.txt
done

# 3. Pod resources
echo "→ Pod resources..."
kubectl top pods -n <flink-namespace> > $OUTPUT/pod_resources.txt 2>/dev/null

# 4. Git info
echo "→ Git state..."
echo "Branch: $(git rev-parse --abbrev-ref HEAD)" > $OUTPUT/git_info.txt
echo "SHA: $(git rev-parse HEAD)" >> $OUTPUT/git_info.txt

echo "Snapshot saved to ./$OUTPUT/"
ls -la $OUTPUT/
```

Run this before deploying new code (save as `snapshot_baseline`) and again after deploying (save as `snapshot_new`). The diff between the two is your comparison data.
