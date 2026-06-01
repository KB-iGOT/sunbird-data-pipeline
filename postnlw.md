# postnlw Deployment Guide

## Objective

Remove the `ingest-router` Flink job from the pipeline. The `telemetry-extractor` becomes the new pipeline entry point, consuming directly from the ingestion Kafka cluster (`telemetry.ingestion`) instead of the intermediate `telemetry.ingest` topic on the processing Kafka cluster.

### Before

```
[Ingestion Kafka]  telemetry.ingestion
        ↓
    ingest-router  (Flink job)
        ↓
[Processing Kafka]  telemetry.ingest
        ↓
  telemetry-extractor  (Flink job)
        ↓
[Processing Kafka]  telemetry.raw  →  ... (pipeline continues unchanged)
```

### After

```
[Ingestion Kafka]  telemetry.ingestion
        ↓
  telemetry-extractor  (Flink job — now reads from ingestion Kafka)
        ↓
[Processing Kafka]  telemetry.raw  →  ... (pipeline continues unchanged)
```

---

## Code Changes (already in this branch)

| File | Change |
|------|--------|
| `data-pipeline-flink/telemetry-extractor/src/main/resources/telemetry-extractor.conf` | Input topic changed from `telemetry.ingest` → `telemetry.ingestion` |
| `kubernetes/helm_charts/datapipeline_jobs/values.j2` | Extractor kafka block: input topic → `telemetry.ingestion`; explicit `consumer.broker-servers = ingestion_kafka_brokers`; explicit `producer.broker-servers = kafka_brokers` |
| `kubernetes/ansible/roles/flink-jobs-deploy/defaults/main.yml` | `ingest-router` → `replica: 0`, `scale_enabled: false`, `min_replica: 0`, `max_replica: 0` |
| `kubernetes/ansible/roles/sunbird-monitoring/templates/dp_prometheus-adapter.yaml` | Extractor HPA metric query topic updated from `telemetry.ingest` → `telemetry.ingestion` |

---

## Ansible Variable Changes

### `kubernetes/ansible/roles/flink-jobs-deploy/defaults/main.yml`

These variables are unchanged but **no longer functionally relevant** (ingest-router is disabled):

```yaml
ingest_router_consumer_parallelism: 2     # ingest-router is disabled — not used
ingest_router_operators_parallelism: 2    # ingest-router is disabled — not used
raw_router_consumer_parallelism: 2        # ingest-router is disabled — not used
raw_router_downstream_parallelism: 2      # ingest-router is disabled — not used
```

The `flink_job_names['ingest-router']` block is now set to:

```yaml
  ingest-router:
    replica: 0
    scale_enabled: false
    min_replica: 0
    max_replica: 0
```

---

## Deployment Steps

### Pre-deployment Checklist

- [ ] `ingestion_kafka_brokers` is correctly set in your environment's Ansible inventory / group_vars (the extractor now consumes from this cluster directly)
- [ ] `kafka_brokers` (processing Kafka) is correctly set — extractor still writes to it
- [ ] Confirm `telemetry.ingestion` topic exists on the ingestion Kafka cluster with correct partition count (16 partitions recommended)
- [ ] Confirm `telemetry.raw` topic exists on the processing Kafka cluster (downstream unchanged)
- [ ] Redis DB 1 is accessible from the extractor taskmanager nodes (dedup store)

### Step 1 — Deploy updated Helm values

Run the Ansible playbook to re-template and apply updated `values.j2`:

```bash
ansible-playbook \
  -i <inventory_file> \
  deploy-datapipeline-jobs.yml \
  -e "job_names_to_deploy=telemetry-extractor" \
  -e "env=<env>"
```

**Do NOT include `ingest-router` in `job_names_to_deploy`.**

### Step 2 — Stop the ingest-router job

If `ingest-router` is currently running, scale it down:

```bash
kubectl scale deployment ingest-router-taskmanager \
  --replicas=0 \
  -n flink-<env>

kubectl delete job.batch ingest-router-jobmanager \
  -n flink-<env>
```

Verify it is stopped:

```bash
kubectl get pods -n flink-<env> | grep ingest-router
# Should return no results
```

### Step 3 — Deploy updated telemetry-extractor

The Helm upgrade in Step 1 already updates the extractor config (new input topic + broker overrides). After the playbook completes, verify the new config is live in the ConfigMap:

```bash
kubectl get configmap telemetry-extractor -n flink-<env> -o yaml | grep "input.topic"
# Expected: input.topic = <env>.telemetry.ingestion

kubectl get configmap telemetry-extractor -n flink-<env> -o yaml | grep "consumer.broker-servers"
# Expected: consumer.broker-servers = "<ingestion_kafka_brokers_value>"
```

Restart the extractor pods to pick up the new config:

```bash
kubectl rollout restart deployment telemetry-extractor-taskmanager \
  -n flink-<env>

kubectl delete job.batch telemetry-extractor-jobmanager \
  -n flink-<env>
```

Wait for the jobmanager to recreate and the taskmanager to come up:

```bash
kubectl get pods -n flink-<env> | grep telemetry-extractor
```

### Step 4 — Deploy updated monitoring

Re-deploy the prometheus-adapter config (updated HPA metric query):

```bash
ansible-playbook \
  -i <inventory_file> \
  deploy-monitoring.yml \
  -e "env=<env>"
```

---

## Verification

### Confirm extractor is consuming from the right topic

Run this against the **ingestion Kafka** (`ingestion_kafka_brokers`) — that is where `telemetry.ingestion` lives:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server <ingestion_kafka_brokers> \
  --group <env>-telemetry-extractor-group \
  --describe
# Expected: Topic column shows <env>.telemetry.ingestion, CONSUMER-ID column shows active members
```

### Confirm old path is dead (processing Kafka)

Run this against the **processing Kafka** (`kafka_brokers`). You should see the group with no active members — that is correct and expected. It shows the historical offsets for the old `telemetry.ingest` topic which the extractor no longer consumes.

```bash
kafka-consumer-groups.sh \
  --bootstrap-server <kafka_brokers> \
  --group <env>-telemetry-extractor-group \
  --describe
# Expected output (correct — means old path is dead):
#   Consumer group '<env>-telemetry-extractor-group' has no active members.
#   TOPIC                     PARTITION  LAG
#   <env>.telemetry.ingest    0          <some number>   <- stale, harmless
```

If the stale lag on `telemetry.ingest` is a concern, reset it:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server <kafka_brokers> \
  --group <env>-telemetry-extractor-group \
  --topic <env>.telemetry.ingest \
  --reset-offsets --to-latest --execute
```

### Confirm ingest-router consumer group is gone

```bash
# Run against ingestion Kafka — should show no active members or unknown group
kafka-consumer-groups.sh \
  --bootstrap-server <ingestion_kafka_brokers> \
  --group <env>-ingest-router-group \
  --describe
```

### Confirm downstream pipeline is healthy

Check that events flow through to `telemetry.raw` and onward:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server <kafka_brokers> \
  --group <env>-pipeline-preprocessor-group \
  --describe
# Lag should be stable / draining
```

### Check extractor Flink job UI

```bash
kubectl port-forward svc/telemetry-extractor-jobmanager 8081:80 -n flink-<env>
# Open http://localhost:8081 — job should be RUNNING, no checkpointing failures
```

---

## Rollback

If issues arise, revert by:

1. Redeploy `ingest-router`:

```bash
ansible-playbook \
  -i <inventory_file> \
  deploy-datapipeline-jobs.yml \
  -e "job_names_to_deploy=ingest-router" \
  -e "env=<env>"
```

2. Redeploy `telemetry-extractor` from the previous `cbrelease-4.8.31` image/config (which reads from `telemetry.ingest`).

---

## Impact Assessment

| Job | Impact |
|-----|--------|
| `ingest-router` | **Removed** — no longer deployed |
| `telemetry-extractor` | **Entry point changed** — reads `telemetry.ingestion` from ingestion Kafka; all outputs unchanged |
| `pipeline-preprocessor` | **No change** — still reads `telemetry.raw` |
| `cb-preprocessor` | **No change** |
| All denorm jobs | **No change** |
| `druid-validator` | **No change** |
| HPA for extractor | Updated: metric now tracks lag on `telemetry.ingestion` |
| Alerting | `ingest_router_*` alerts will no longer fire (job removed); no alert rule file changes needed |

---

## Notes

- The `telemetry.ingest` topic on processing Kafka will become idle. It can be retained for a grace period and deleted later after confirming no consumers depend on it.
- The `ingest-router` HPA metric (`ingest-router_kafka_consumergroup_lag_sum`) in `dp_prometheus-adapter.yaml` is now stale but harmless — it will simply return zero/empty. It can be cleaned up in a follow-up.
- Alert thresholds for `ingest_router_threshold_critical` / `ingest_router_threshold_warning` in `sunbird-monitoring/defaults/main.yml` are inert once the job is stopped. Clean them up in a follow-up maintenance cycle.
