# Code Removals Log

Tracks intentional removal of logic from pipeline jobs — what was removed, why, which files were affected, and the downstream impact. Each section covers one job. Entries within a section are listed in reverse chronological order.

---

## pipeline-preprocessor

### Removal: Data-correction logic in telemetry validation

**Date:** 2026-06-18  
**Branch:** cbrelease-4.8.38

#### What was removed

**1. `f:` prefix stripping from federated user actor IDs**

Actor IDs of the form `f:<orgId>:<userId>` were stripped to just the last segment (`<userId>`) before the event was forwarded downstream.

- Input actor ID: `f:01285019302823526477:610bab7d-1450-4e54-bf78-c7c9b14dbc81`
- Old output: `610bab7d-1450-4e54-bf78-c7c9b14dbc81`
- New output: actor ID passed through unchanged

**2. `dialCodes` → `dialcodes` key rename in SEARCH events**

For SEARCH events (`eid = "SEARCH"`), if `edata.filters.dialCodes` was present it was copied to `edata.filters.dialcodes` (lowercase `c`) and the original key was nulled out.

**3. dialCode value uppercasing for DialCode/QR object types**

For events where `object.type` was `DialCode` or `qr`, the `object.id` value was converted to uppercase.

- Input `object.id`: `gwni38`
- Old output: `GWNI38`
- New output: value passed through unchanged

#### Why removed

These corrections were compensating for upstream client-side inconsistencies that have since been fixed at the source. Keeping them in the pipeline created a mismatch between what clients send and what is stored, making data lineage harder to trace and complicating re-processing of historical events.

#### Files affected

| File | Change |
|------|--------|
| `pipeline-preprocessor/src/main/scala/.../functions/TelemetryValidationFunction.scala` | Removed `dataCorrection()` private method and its call in `onValidationSuccess` |
| `pipeline-preprocessor/src/main/scala/.../util/TelemetryValidator.scala` | Removed `dataCorrection()` private method and its call in `onValidationSuccess` |
| `pipeline-preprocessor/src/main/scala/.../domain/Event.scala` | Removed `updateActorId()`, `correctDialCodeKey()`, and `correctDialCodeValue()` methods |

#### Downstream impact

- Events with `f:`-prefixed actor IDs now carry the full federated ID into denorm, druid, and all downstream jobs
- SEARCH events with `dialCodes` in edata.filters retain the original key name
- DialCode/QR events retain the original casing of `object.id`
- No changes to validation logic, routing, or deduplication

#### Tests

`PipelineProcessorStreamTaskTestSpec` was not changed. Its assertions cover event counts and metric counters, which are unaffected by this removal.

---

## ingest-router (retired)

### Removal: ingest-router retired from pipeline flow

**Date:** 2026-06-19  
**Branch:** cbrelease-4.8.38

#### What was removed

The `ingest-router` Flink job was a simple pass-through that read raw bytes from `{env}.telemetry.ingestion` and forwarded them unchanged to `{env}.telemetry.ingest`. No transformation, validation, or enrichment was applied. The module is removed from the parent `pom.xml`.

#### Why removed

The `telemetry-extractor` job already reads directly from `{env}.telemetry.ingestion` — it never consumed from `{env}.telemetry.ingest`. This means ingest-router's output was going to an unconsumed topic, making the job a redundant consumer that added lag and resource cost with zero effect on the pipeline. The flow is:

```
Before: telemetry.ingestion → [ingest-router] → telemetry.ingest (unread)
                           ↘ [telemetry-extractor]

After:  telemetry.ingestion → [telemetry-extractor]
```

#### Modules retired (removed from parent pom)

| Module | Was consuming |
|--------|--------------|
| `ingest-router` | `{env}.telemetry.ingestion` → wrote to `{env}.telemetry.ingest` (unread by any job) |

#### Files affected

| File | Change |
|------|--------|
| `data-pipeline-flink/pom.xml` | Removed `ingest-router` module entry |

#### Downstream impact

- `{env}.telemetry.ingest` topic will receive no new messages
- `{env}.telemetry.ingestion` consumer group `{env}-ingest-router-group` is gone; no lag accumulation
- Pipeline flow is unchanged — telemetry-extractor continues to read from `{env}.telemetry.ingestion` directly

---

## pipeline-preprocessor + telemetry-extractor + retired modules

### Restoration: AUDIT event routing re-enabled (CB_AUDIT remains dropped)

**Date:** 2026-08-10  
**Branch:** cbrelease-4.8.38

#### What changed

AUDIT event routing in `pipeline-preprocessor` is restored to exactly how it worked before the removal below — AUDIT events are no longer dropped. CB_AUDIT and SHARE remain dropped; ASSESS/RESPONSE remain dropped at `telemetry-extractor`. None of that is affected by this change.

**1. AUDIT event routing (`eid = "AUDIT"`) — pipeline-preprocessor**

AUDIT events now flow through dedup like any other event, then are routed to **both** `{env}.telemetry.audit` and the primary route topic (`{env}.telemetry.unique`) — same as pre-removal behavior. The `primary-route-success-count` and `audit-route-success-count` metrics both increment for AUDIT events (as they did originally).

**2. `user-cache-updater-2.0` revived**

The topic `{env}.telemetry.audit` has exactly one consumer: `user-cache-updater-2.0`. It was retired (removed from the parent pom, code left on disk) in the same commit that dropped AUDIT. It's added back to `data-pipeline-flink/pom.xml` so the topic is actually consumed again instead of accumulating unread lag.

#### Why changed

AUDIT events are needed again; CB_AUDIT (work-order) and ASSESS/RESPONSE (aggregation/certificates) are still not needed and stay dropped as before.

#### Files affected

| File | Change |
|------|--------|
| `pipeline-preprocessor/src/main/scala/.../functions/PipelinePreprocessorFunction.scala` | Removed `"AUDIT"` from the drop-branch condition (CB_AUDIT/SHARE still drop); restored `case "AUDIT"` in the eid match (routes to `auditRouteEventsOutputTag`, increments `auditEventRouterMetricCount` + `primaryRouterMetricCount`); restored `auditEventRouterMetricCount` in `metricsList()` |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorConfig.scala` | Restored `kafkaAuditRouteTopic`, `auditRouteEventsOutputTag`, `auditEventRouterMetricCount`, `auditRouterProducer`, `auditEventsPrimaryRouteProducer` |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorStreamTask.scala` | Restored the two AUDIT sinks (`{env}.telemetry.audit` and `{env}.telemetry.unique`) reading from `auditRouteEventsOutputTag` |
| `pipeline-preprocessor/src/main/resources/pipeline-preprocessor.conf` | Restored `output.audit.route.topic = ${job.env}".telemetry.audit"` |
| `pipeline-preprocessor/src/test/resources/test.conf` | Restored `output.audit.route.topic` |
| `pipeline-preprocessor/src/test/scala/.../spec/PipelineProcessorStreamTaskTestSpec.scala` | Restored `TelemetryAuditEventSink`, its mock wiring, and assertions; sink/metric counts updated (primary +1, audit +1, unique-event +1) |
| `data-pipeline-flink/pom.xml` | Restored `<module>user-cache-updater-2.0</module>` (assessment-aggregator and cb-preprocessor remain retired) |
| `kubernetes/helm_charts/datapipeline_jobs/values.j2` | Restored `output.audit.route.topic = {{ env_name }}.telemetry.audit` in the `pipeline-preprocessor:` conf block — **this is the config that's actually mounted into the running job** (see note below), separate from the jar's bundled `pipeline-preprocessor.conf` |

#### Downstream impact

- AUDIT events flow to `{env}.telemetry.audit` (consumed again by `user-cache-updater-2.0`) and to `{env}.telemetry.unique` (same path as every other non-LOG/ERROR/CB_AUDIT/SHARE event — currently `druid-events-validator` reads this directly since `denorm.enabled = false`; see the denorm-bypass entry below)
- CB_AUDIT and SHARE events are still silently dropped after validation — unaffected by this change
- ASSESS/RESPONSE events are still dropped at `telemetry-extractor` — unaffected by this change
- Cassandra `user` cache updates driven by `user-cache-updater-2.0` resume

#### Ansible / Helm variables

**There are two separate copies of `pipeline-preprocessor.conf` in this repo, and both need the new key.** `pipeline-preprocessor/src/main/resources/pipeline-preprocessor.conf` is only the fallback used when the job runs without `--config.file.path`. The actual deployed job is started with `--config.file.path /data/flink/conf/pipeline-preprocessor.conf` ([flink_job_deployment.yaml](../kubernetes/helm_charts/datapipeline_jobs/templates/flink_job_deployment.yaml)), which is mounted from a ConfigMap ([flink_job_configmap.yaml](../kubernetes/helm_charts/datapipeline_jobs/templates/flink_job_configmap.yaml)) rendered from `{{ index .Values $name }}` — i.e. from `values.yaml`, which Ansible templates from **`kubernetes/helm_charts/datapipeline_jobs/values.j2`**, not from the source resource file. Missing this the first time around caused `ConfigException$Missing: No configuration setting found for key 'kafka.output.audit'` at job startup. Both copies are now updated.

Beyond that: `kubernetes/ansible/roles/flink-jobs-deploy/defaults/main.yml` already carries a `user-cache-updater-v2` entry under `flink_job_names` (`job_class_name: org.sunbird.dp.usercache.task.UserCacheUpdaterStreamTaskV2`) and `values.j2` already has a matching `user-cache-updater-v2:` conf block pointing at `telemetry.audit` — neither was removed when the module was dropped from the pom, so no changes were needed there. Just include `user-cache-updater-v2` in the `job_names_to_deploy` list passed to the `flink-jobs-deploy` role/playbook when deploying.

#### Deployment order

1. Re-run the ansible/helm deploy for `pipeline-preprocessor` — this re-templates `values.yaml` from `values.j2` (picking up the `output.audit.route.topic` fix), regenerates the ConfigMap, and rolls the job. Rebuilding the jar alone is **not** sufficient — the old ConfigMap will still be missing the key until the deploy step re-templates it.
2. Build and deploy `user-cache-updater-2.0` (`job_class_name: org.sunbird.dp.usercache.task.UserCacheUpdaterStreamTaskV2`, ansible catalog key `user-cache-updater-v2`) so the topic doesn't accumulate lag between steps 1 and 2
3. No changes needed to `druid-events-validator`, `telemetry-extractor`, or any other job

#### Tests

- `PipelineProcessorStreamTaskTestSpec`: restored `TelemetryAuditEventSink` sink/mock and its assertion; `TelemetryPrimaryEventSink` 5→6, `primaryRouterMetricCount` 5→6, `unique-event-count` 6→7, `auditEventRouterMetricCount` asserts 1. CB_AUDIT/SHARE-related counts (already 0/unchanged since that removal) are untouched.
- Not run locally in this session — no JDK 11 available in this sandbox (project targets Java 11 per `data-pipeline-flink/pom.xml`'s `java.target.runtime`, only JDK 17/25 installed here). Run `mvn test` in the normal CI/build environment before merging.

---

### Removal: AUDIT, CB_AUDIT, and ASSESS/RESPONSE event processing

**Date:** 2026-06-19  
**Branch:** cbrelease-4.8.38

#### What was removed

**1. AUDIT event routing (`eid = "AUDIT"`) — pipeline-preprocessor**

AUDIT events were routed to `{env}.telemetry.audit` (for user-cache updates) and also sent to the primary route topic (denorm). Now dropped entirely after validation — they never reach dedup or any downstream topic.

**2. CB_AUDIT event routing (`eid = "CB_AUDIT"`) — pipeline-preprocessor**

CB_AUDIT events were routed to `{env}.telemetry.cb.audit` for downstream work-order processing. Now dropped entirely after validation.

**3. ASSESS and RESPONSE event redaction pipeline — telemetry-extractor**

ASSESS and RESPONSE events were specially routed through a `RedactorFunction` that checked question type from a Redis content cache. Registration-type questions had sensitive fields (`resvalues`/`values`) cleared before forwarding. All other ASSESS/RESPONSE events went to `{env}.telemetry.assess.raw` and then to `{env}.telemetry.assess` for the assessment-aggregator. These events are now dropped at the extraction stage and never enter telemetry.raw.

#### Why removed

These event types and their downstream processing are no longer used:
- CB_AUDIT was tied to work-order functionality, which has been retired
- AUDIT events fed user-cache-updater-2.0, which provided cache warming no longer needed
- ASSESS/RESPONSE aggregation (assessment scores, certificate issuance) is no longer active

#### Modules retired (removed from parent pom)

| Module | Was consuming |
|--------|--------------|
| `assessment-aggregator` | `{env}.telemetry.assess` → wrote to Cassandra `assessment_aggregator`, `user_activity_agg`; produced `{env}.issue.certificate.request` |
| `cb-preprocessor` | `{env}.telemetry.cb.audit` → produced `{env}.druid.cb.audit`, `{env}.druid.cb.work.order.row`, `{env}.druid.cb.work.order.officer` |
| `user-cache-updater-2.0` | `{env}.telemetry.audit` exclusively — no other purpose |

#### Files affected

| File | Change |
|------|--------|
| `pipeline-preprocessor/src/main/scala/.../functions/PipelinePreprocessorFunction.scala` | Added drop branch for AUDIT/CB_AUDIT; removed both cases from eid match; removed metrics |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorStreamTask.scala` | Removed 3 audit sinks; removed dead TelemetryRouterFunction import |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorConfig.scala` | Removed audit/cb-audit topics, output tags, metric names, producer names |
| `pipeline-preprocessor/src/main/resources/pipeline-preprocessor.conf` | Removed `output.audit.route.topic`, `output.cb.audit.route.topic` |
| `pipeline-preprocessor/src/main/scala/.../functions/TelemetryRouterFunction.scala` | **Deleted** — dead code, never wired into any stream task |
| `telemetry-extractor/src/main/scala/.../functions/ExtractionFunction.scala` | ASSESS/RESPONSE events now dropped; removed `redactEventsList` check |
| `telemetry-extractor/src/main/scala/.../task/TelemetryExtractorStreamTask.scala` | Removed `RedactorFunction` stream step and assess-raw sinks |
| `telemetry-extractor/src/main/scala/.../task/TelemetryExtractorConfig.scala` | Removed assess topics, redact config, cache metric fields |
| `telemetry-extractor/src/main/resources/telemetry-extractor.conf` | Removed `output.assess.raw.topic`, `redis-meta` block, `redact.events.list` |
| `telemetry-extractor/src/main/scala/.../functions/RedactorFunction.scala` | **Deleted** — only purpose was ASSESS/RESPONSE redaction |
| `data-pipeline-flink/pom.xml` | Removed `assessment-aggregator`, `user-cache-updater-2.0`, `cb-preprocessor` module entries |

#### Downstream impact

- AUDIT events are silently dropped after validation; they no longer flow to denorm, druid, or any downstream job
- CB_AUDIT events are silently dropped after validation; `telemetry.cb.audit` topic will receive no new messages
- ASSESS and RESPONSE events are dropped at extraction; `telemetry.assess.raw` and `telemetry.assess` topics will receive no new messages
- Certificate issuance triggered by assessment completion is no longer produced
- Cassandra tables `assessment_aggregator` and `user_activity_agg` are no longer updated by this pipeline

#### Deployment order (to avoid lag)

1. Deploy updated `pipeline-preprocessor` and `telemetry-extractor` first — stops production to audit/cb-audit/assess topics
2. Monitor consumer group lag on `telemetry.audit`, `telemetry.cb.audit`, `telemetry.assess.raw` — wait for drain
3. Shut down and decommission `user-cache-updater-2.0`, `cb-preprocessor`, `assessment-aggregator`

#### Tests

- `PipelineProcessorStreamTaskTestSpec`: removed `TelemetryAuditEventSink`, removed audit mock/assertions, updated sink counts (primary −1, denorm-primary −1) and metric counters (primary-route −1, unique-event −1, denorm-primary −1)
- `TelemetryExtractionStreamTaskTestSpec`: removed `AssessRawEventsSink`, Redis setup, and all redactor assertions; updated `RawEventsSink` (6→1) and `successEventCount` (44→39)

---

### Removal: SHARE event flattening and SHARE_ITEM routing

**Date:** 2026-06-19  
**Branch:** cbrelease-4.8.38

#### What was removed

**1. SHARE event flattening (`eid = "SHARE"`) — pipeline-preprocessor**

SHARE events were expanded into one or more SHARE_ITEM events by `ShareEventsFlattener`. Each item in `edata.items` became a separate SHARE_ITEM event with its own `object.id`, `object.type`, and an `edata.type` of `"download"` (transfers = 0) or `"import"` (transfers > 0). Both the original SHARE event and all generated SHARE_ITEM events were routed downstream to denorm and the primary topic. Now SHARE events are dropped immediately after validation, before deduplication.

**2. SHARE_ITEM secondary-event routing**

`SHARE_ITEM` was listed in `secondary.events` so that generated SHARE_ITEM events were routed to `{env}.telemetry.unique.secondary` (the high-priority denorm lane). This entry has been removed. `secondary.events` now contains only `["INTERACT", "IMPRESSION"]`.

#### Why removed

There are no active producers emitting SHARE events. The flattening logic, config fields, output tags, and sinks were dead code.

#### Files affected

| File | Change |
|------|--------|
| `pipeline-preprocessor/src/main/scala/.../functions/PipelinePreprocessorFunction.scala` | Added SHARE to the drop chain (with AUDIT/CB_AUDIT); removed `shareEventsFlattener` field, `open()` init, and `case "SHARE"` match; removed share metrics from `metricsList()` |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorStreamTask.scala` | Removed `ShareEventsFlattenerFunction` import; removed `shareItemEventOutputTag` sink |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorConfig.scala` | Removed `shareRouteEventsOutputTag`, `shareItemEventOutputTag`, `SHARE_EVENTS_FLATTEN_FLAG_NAME`, `shareEventsRouterMetricCount`, `shareItemEventsMetircsCount`, `shareEventsFlattenerFunction`, `shareEventsPrimaryRouteProducer`, `shareItemsPrimaryRouterProducer` |
| `pipeline-preprocessor/src/main/resources/pipeline-preprocessor.conf` | Removed `SHARE_ITEM` from `secondary.events` |
| `pipeline-preprocessor/src/main/scala/.../util/ShareEventsFlattener.scala` | **Deleted** — only purpose was SHARE → SHARE_ITEM flattening |
| `pipeline-preprocessor/src/main/scala/.../functions/ShareEventsFlattenerFunction.scala` | **Deleted** — duplicate of above, not wired into any stream task |
| `pipeline-preprocessor/src/main/scala/.../domain/Models.scala` | **Deleted** — contained `ShareEvent` and supporting case classes (`EData`, `Rollup`, `EventObject`, `ActorObject`, `Context`) used exclusively by the two deleted flattener files |

#### Downstream impact

- SHARE events are silently dropped after validation; they no longer flow to denorm, the primary route topic, or any other downstream job
- SHARE_ITEM events are no longer generated; `telemetry.unique.secondary` will not receive SHARE_ITEM events
- No other Flink job had special handling for SHARE or SHARE_ITEM — denorm, druid-events-validator, and others treated them as generic events

#### Tests

- `PipelineProcessorStreamTaskTestSpec`: removed `SHARE_ITEM_EVENT` case class, removed SHARE_ITEM count assertion, removed `expectedShareItems` assertion block, removed `shareEventsRouterMetricCount` and `shareItemEventsMetircsCount` metric assertions; updated sink counts (`TelemetryPrimaryEventSink` 9→5, `TelemetryDenormSecondaryEventSink` 4→1, `TelemetryDenormPrimaryEventSink` 5→4) and metric counters (`primaryRouterMetricCount` 6→5, `unique-event-count` 7→6, `denormSecondaryEventsRouterMetricsCount` 4→1, `denormPrimaryEventsRouterMetricsCount` 5→4)

---

---

## de-normalization + pipeline-preprocessor + druid-events-validator

### Change: Bypass de-normalization job via `denorm.enabled` flag

**Date:** 2026-06-19  
**Branch:** cbrelease-4.8.38

#### What changed

**1. `denorm.enabled` flag added to both jobs**

A boolean flag `denorm.enabled` (default: `false`) was added to the configs of `pipeline-preprocessor` and `druid-events-validator`. Flipping it to `true` in both jobs restores the original enrichment pipeline.

**2. `pipeline-preprocessor` — denorm priority-split sinks disabled**

When `denorm.enabled = false`, the preprocessor no longer writes events to `{env}.telemetry.unique.primary` (all non-INTERACT/IMPRESSION events) or `{env}.telemetry.unique.secondary` (INTERACT/IMPRESSION events). All events continue to flow as normal to `{env}.telemetry.unique` (the primary route topic). The `denorm.enabled` guard was added to both the routing logic in `PipelinePreprocessorFunction` and the sink registration in `PipelinePreprocessorStreamTask`.

**3. `druid-events-validator` — input topic switched to `telemetry.unique`**

When `denorm.enabled = false`, the validator reads from `{env}.telemetry.unique` (bypassing the de-normalization job entirely). When `denorm.enabled = true`, it reads from `{env}.telemetry.denorm` as before. The config retains both topic keys (`kafka.input.topic` and `kafka.input.denorm.topic`) for easy switching.

#### Why changed

The de-normalization job (device/user/content/dialcode/location enrichment via Redis) was deactivated after review. Its output is no longer required downstream. Keeping it as a configurable bypass (rather than outright removal) preserves the ability to re-enable enrichment with a single flag change per job.

#### Files affected

| File | Change |
|------|--------|
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorConfig.scala` | Added `isDenormEnabled: Boolean` field |
| `pipeline-preprocessor/src/main/scala/.../functions/PipelinePreprocessorFunction.scala` | Denorm routing branches gated on `config.isDenormEnabled` |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorStreamTask.scala` | Denorm sink registration wrapped in `if (config.isDenormEnabled)` |
| `pipeline-preprocessor/src/main/resources/pipeline-preprocessor.conf` | Added `denorm.enabled = false` |
| `druid-events-validator/src/main/resources/druid-events-validator.conf` | Added `denorm.enabled = false`; changed `kafka.input.topic` to `telemetry.unique`; added `kafka.input.denorm.topic = telemetry.denorm` |
| `druid-events-validator/src/main/scala/.../task/DruidValidatorConfig.scala` | Added `isDenormEnabled`; `kafkaInputTopic` now selects between `kafka.input.topic` and `kafka.input.denorm.topic` based on the flag |

#### Downstream impact

- `{env}.telemetry.unique.primary` and `{env}.telemetry.unique.secondary` will receive no new messages (de-normalization job can be shut down and decommissioned)
- Events entering `druid-events-validator` are un-enriched — no `devicedata`, `userdata`, `contentdata`, `dialcodedata`, or `locationdata` fields
- `{env}.telemetry.denorm` will receive no new messages

#### How to re-enable

Set `denorm.enabled = true` in both job configs:

```
# pipeline-preprocessor.conf
denorm.enabled = true

# druid-events-validator.conf
denorm.enabled = true
```

Redeploy both jobs. The de-normalization job must also be running and consuming from `{env}.telemetry.unique`.

#### Tests

- `PipelineProcessorStreamTaskTestSpec`: `TelemetryDenormSecondaryEventSink` and `TelemetryDenormPrimaryEventSink` now assert 0 events; `denormSecondaryEventsRouterMetricsCount` and `denormPrimaryEventsRouterMetricsCount` metrics assert 0; removed mock setups for denorm sinks
- `DruidValidatorStreamTaskTestSpec`: no assertion changes — same test events flow through, only the source topic resolves differently via `kafkaInputTopic`

---

---

## pipeline-preprocessor

### Change: Skip LOG events via `log.events.skip` flag

**Date:** 2026-06-19  
**Branch:** cbrelease-4.8.38

#### What changed

A boolean flag `log.events.skip` (default: `true`) was added to the preprocessor. When `true`, LOG events (`eid = "LOG"`) are silently dropped immediately after validation — no side-output is written, no metric is incremented, and the `druid.events.log` Kafka sink is not registered. When `false`, LOG events are processed and routed exactly as before.

#### Why changed

The `{env}.druid.events.log` topic is not consumed by any downstream Flink job in this pipeline. The routing logic is kept in place so it can be re-enabled with a single flag flip if an external consumer or a new downstream job needs LOG events again.

#### Files affected

| File | Change |
|------|--------|
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorConfig.scala` | Added `skipLogEvents: Boolean` field |
| `pipeline-preprocessor/src/main/scala/.../functions/PipelinePreprocessorFunction.scala` | LOG routing wrapped in `if (!config.skipLogEvents)` guard |
| `pipeline-preprocessor/src/main/scala/.../task/PipelinePreprocessorStreamTask.scala` | LOG sink registration wrapped in `if (!config.skipLogEvents)` guard |
| `pipeline-preprocessor/src/main/resources/pipeline-preprocessor.conf` | Added `log.events.skip = true` |

#### Downstream impact

- `{env}.druid.events.log` will receive no new messages from the preprocessor
- No other Flink job is affected — LOG events never entered the dedup or primary-route flow

#### How to re-enable

Set `log.events.skip = false` in `pipeline-preprocessor.conf` and redeploy.

#### Tests

- `PipelineProcessorStreamTaskTestSpec`: `TelemetryLogEventSink` now asserts 0 events; `logEventsRouterMetricsCount` metric asserts 0; removed mock setup for the log sink. All other sink counts and metrics are unchanged.

---

<!-- To add a new job section, copy the template below and fill it in:

## <job-name>

### Removal: <short description>

**Date:** YYYY-MM-DD
**Branch:** <branch-name>

#### What was removed

#### Why removed

#### Files affected

| File | Change |
|------|--------|

#### Downstream impact

#### Tests

-->
