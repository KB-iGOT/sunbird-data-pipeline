# Sunbird Data Pipeline (branch: release-3.6.0)

The purpose of data-pipeline is to process the Telemetry (+ some other) data, and store it for use by
anyone who requires it, in a scalable manner.

**Note:** this document assumes default configuration of flink jobs, actual configuration for each of the flink jobs is present [here](../kubernetes/helm_charts/datapipeline_jobs/values.j2)

**Note:** To troubleshoot your data-pipeline setup see doc [here](dp-debug.md "Data-pipeline debug")

## Overview

Batch events are sent to Kafka Topic `env.telemetry.ingest` by the Telemetry API. From there a series of Flink jobs
transform this stream, first by de-duplicating and extracting individual events, generating `AUDIT` meta events, routing
different event types to different Kafka topics, de-normalizing data present in the stream, validating the data stream
for Druid ingestion, and sending this processed stream to `env.druid.*` Kafka topics. From there Druid ingests
the data using its Kafka-Ingestion service into *raw* and *rollup* clusters. The data stored in Druid is then available
to be viewed/visualized/analyzed using Superset. Each of the Kafka topics is backed up to Azure Blob Storage using a
Secor Process.

## Components

Sunbird Data Pipeline majorly consists of following Components
- [Data Source - Telemetry API - Node.js](#telemetry-api---nodejs) emits individual or batch events to Kafka Ingestion Topic
- [Transport - Apache Kafka](#apache-kafka---transport) for reliably moving data among various services
- [Transport Backup - Azure Blob Storage](#backup---secor---azure-blob-store) to back-up Kafka topics to Azure Blob Storage using a Secor Process
- [Stream Processing - Apache Flink](#apache-flink---stream-processing) i.e. De-duplication, De-normalization, Routing etc.
- [Batch Processing - Apache Spark](#apache-spark---batch-processing) for batch processing
- [OLAP Store - Apache Druid](#druid---olap-store) for storing processed data in raw and rollup clusters
- [Data Visualization - Superset](#superset) to access, visualize and analyze data present in Druid

### Telemetry API - Node.js

REST API implemented in Node.js, and used to send individual/batch events to Kafka Ingestion Topic. More details
regarding this can be found [here](http://docs.sunbird.org/latest/developer-docs/telemetry/specification/ "Telemetry Specs")

### Apache Kafka - Transport

Kafka is utilized to move data among different data-pipeline services, following table shows the various Kafka topics
and their purpose

#### Kafka Topics

Kafka Topic (env.)              | Topic Content             | Topic Consumers           | Topic Producers
--------------------------------|---------------------------|---------------------------|----------------------------
telemetry.ingest                | Batch-Events              | telemetry-extractor       | Telemetry API
telemetry.extractor.duplicate   | Duplicate Batch-Events    |                           | telemetry-extractor
telemetry.extractor.failed      | Failed Batch-Events       |                           | telemetry-extractor
telemetry.raw                   | Unique Events             | pipeline-preprocessor     | telemetry-extractor
telemetry.assess.raw            | ASSES/RESPONSE Events     |                           | telemetry-extractor
telemetry.error                 | ERROR Events              |                           | pipeline-preprocessor
telemetry.audit                 | AUDIT Events              | user-cache-updater        | pipeline-preprocessor
telemetry.duplicate             | Duplicate Events          |                           | pipeline-preprocessor,<br> de-normalization:Summary,<br> druid-events-validator
telemetry.unique                | Unique Events             | de-normalization          | pipeline-preprocessor
telemetry.unique.secondary      | Secondary Unique Events   |                           | pipeline-preprocessor
telemetry.unique.primary        | Unique Events             |                           | pipeline-preprocessor
telemetry.denorm                | De-normalized Events      | druid-events-validator    | de-normalization
druid.events.summary            | Summary Events            | Druid                     | de-normalization:Summary,<br> druid-events-validator
telemetry.derived               | Summary Events            | de-normalization:Summary  | Spark
telemetry.derived.unique        | Unique Summary Events     |                           | de-normalization:Summary
druid.events.telemetry          | Validated Events          | Druid                     | druid-events-validator
telemetry.assess?               | ASSES/RESPONSE Events?    | assessment-aggregator     | ?
telemetry.assess.failed         |                           |                           | assessment-aggregator
issue.certificate.request       |                           |                           | assessment-aggregator
learning.graph.events           |                           | content-cache-updater     | ?
events.deviceprofile            |                           | device-profile-updater    | ?
telemetry.failed                | Failed Events             |                           | telemetry-extractor,<br> pipeline-preprocessor,<br> de-normalization,<br> druid-events-validator
druid.events.log                | LOG events                | Druid                     | telemetry-extractor,<br> pipeline-preprocessor

#### Backup - Secor - Azure Blob Store

Kafka topics are backed up to Azure Blob Store using Secor Processes
[expand]

### Apache Flink - Stream Processing

#### Core Components `dp-core`

Common components shared among all other components of Flink Data Pipeline

`domain` - package containing event related classes

`reader` - telemetry reading utilities

`serde` - package containing Serialization/De-Serialization utilities for `String`, `Byte`, `Map` and `Event`

`cache` - package containing utilities for connecting to and querying redis store
- `RedisConnect` - A Utility for providing connection to redis via `Jedis`
- `DataCache` - Utility to store and retrieve objects from a redis store of given index
- `DeDupeEngine` - De-Duplication Utility - provides helper methods to query de-duplication redis store to check/store a checksum

`job` - package containing job related base classes and Flink-Kafka connection utility
- `BaseDuplication` - Base class (trait) providing de-duplication functionality,
- `BaseJobConfig` - Base class for job config
- `BaseProcessFunction` - Contains abstract classes for `ProcessFunction`s in the data-pipeline
    - `JobMetrics` - Trait providing functionality to modify/retrieve job metrics in a thread safe manner
    - `BaseProcessFunction` - abstract class extending `ProcessFunction`, `BaseDuplication` and `JobMetrics` takes config
      as input, all data-pipeline `ProcessFunction`s inherit from this
    - `WindowBaseProcessFunction` - abstract class extending `WindowProcessFunction`, `BaseDuplication` and `JobMetrics` takes config
      as input, all data-pipeline `WindowProcessFunction`s inherit from this
- `FlinkKafkaConnector` - A Utility providing methods for creating Kafka sources and sinks for flink jobs, utilises `serde`
  package for Serialization/De-Serialization functionality

`util`
- `CassandraUtil` - Utility to connect to and query Cassandra
- `FlinkUtil` - Utility to provide `StreamExecutionEnvironment` with job specific config applied
- `JSONUtil` - Utility to help with JSON Serialization/De-Serialization
- `PostgresConnect` - Utility to connect to and query Postgres
- `RestUtil` - Utility for calling REST API

#### Flink Jobs

Sunbird Flink Job Projects follow the following package structure (mostly) -
1) `domain` - Contains all the models relevant to the job - POJO's or Scala case classes
2) `functions` - Contains Flink `ProcessFunction`s for user defined stream transformations
3) `task` - Contains Flink Tasks to be run and Config associated with them
4) `util` - Helper classes/functions
5) other packages

*Note* - In the following documentation `ProcessFunction` subclasses are frequently attributed to routing messages that
satisfy a certain criterion to specific/multiple kafka topics. In reality, `ProcessFunction`s do not perform
the actual routing, they tag messages that satisfy a certain criterion with respective `OutputTag`s, and it's the
`StreamTask` that routes messages with these `OutputTag`s to respective kafka topics. This bit of detail has been
skipped to facilitate better understanding of the underlying logic.


#### Telemetry Extractor `telemetry-extractor`

Config variable         | Config key                        | Config value
------------------------|-----------------------------------|-----------------------------------
`kafkaInputTopic`       | kafka.input.topic                 | env.telemetry.ingest
`kafkaDuplicateTopic`   | kafka.output.duplicate.topic      | env.telemetry.extractor.duplicate
`kafkaBatchFailedTopic` | kafka.output.batch.failed.topic   | env.telemetry.extractor.failed
`kafkaSuccessTopic`     | kafka.output.success.topic        | env.telemetry.raw
`kafkaLogRouteTopic`    | kafka.output.log.route.topic      | env.druid.events.log
`kafkaFailedTopic`      | kafka.output.failed.topic         | env.telemetry.failed
`kafkaAssessRawTopic`   | kafka.output.assess.raw.topic     | env.telemetry.assess.raw
`redactEventsList`      | redact.events.list                | ASSESS, RESPONSE


`TelemetryExtractorStreamTask`

This task reads from batch telemetry events from `kafkaInputTopic` and
- handles de-duplication of batch events by extracting message identifier and checking against a redis store
- parses unique batch events into individual events and generates `AUDIT` events
- routes `LOG`/`AUDIT` events and events in `redactEventsList`  to respective kafka topics
- routes invalid/errored out events to kafka failure topics
- routes rest of events to be further processed by [`pipeline-preprocessor`](#pipeline-pre-processor-pipeline-preprocessor)

Source - `kafkaInputTopic`

Sink - `kafkaSuccessTopic`, `kafkaFailedTopic`, `kafkaDuplicateTopic`, `kafkaBatchFailedTopic`, `kafkaAssessRawTopic`, `kafkaLogRouteTopic`

Transformations -
- Reads from source stream then applies `DeDuplicationFunction`
    -  `DeDuplicationFunction`
        - routes failed batch-events to `kafkaBatchFailedTopic`
        - routes duplicate batch-events to `kafkaDuplicateTopic`
        - applies `ExtractionFunction` to the unique batch events
    - `ExtractionFunction`
        - generates `AUDIT` events (to count the events in a batch) which are routed to `kafkaLogRouteTopic`
        - extracts individual events from batch-events
        - routes events with size > `config.maxSize` to `kafkaFailedTopic`
        - routes `LOG` events to `kafkaLogRouteTopic`
        - applies `RedactorFunction` to events in `redactEventsList`
        - routes other events to `kafkaSuccessTopic`
    - `RedactorFunction`
        - routes all events to `kafkaSuccessTopic`
        - routes events with `questionType=Registration` to `kafkaAssessRawTopic`


#### Pipeline Pre-processor `pipeline-preprocessor`

Config variable                 | Config key                                | Config value
--------------------------------|-------------------------------------------|---------------------------------
`kafkaInputTopic`               | kafka.input.topic                         | env.telemetry.raw
`kafkaFailedTopic`              | kafka.output.failed.topic                 | env.telemetry.failed
`kafkaPrimaryRouteTopic`        | kafka.output.primary.route.topic          | env.telemetry.unique
`kafkaLogRouteTopic`            | kafka.output.log.route.topic              | env.druid.events.log
`kafkaErrorRouteTopic`          | kafka.output.error.route.topic            | env.telemetry.error
`kafkaAuditRouteTopic`          | kafka.output.audit.route.topic            | env.telemetry.audit
`kafkaDuplicateTopic`           | kafka.output.duplicate.topic              | env.telemetry.duplicate
`kafkaDenormSecondaryRouteTopic`| kafka.output.denorm.secondary.route.topic | env.telemetry.unique.secondary
`kafkaDenormPrimaryRouteTopic`  | kafka.output.denorm.primary.route.topic   | env.telemetry.unique.primary
`secondaryEvents`               | kafka.secondary.events                    | INTERACT, IMPRESSION, SHARE_ITEM


`PipelinePreprocessorStreamTask`

This task routes data to relevant Kafka topics. It also de-duplicates, validates event schema and flattens `SHARE`
events. Unique events are routed to be processed by [`de-normalization`](#de-normalization-de-normalization)

Source - `kafkaInputTopic`

Sink - `kafkaFailedTopic`, `kafkaPrimaryRouteTopic`, `kafkaLogRouteTopic`, `kafkaErrorRouteTopic`,
`kafkaAuditRouteTopic`, `kafkaDuplicateTopic`, `kafkaDenormSecondaryRouteTopic`, `kafkaDenormPrimaryRouteTopic`

Transformations -
- Reads from source stream then applies `PipelinePreprocessorFunction`
    - `PipelinePreprocessorFunction`
        - if schema is missing or if validation fails events are routed to `kafkaFailedTopic`
        - routes `LOG` events to `kafkaLogRouteTopic`
        - routes duplicate events to `kafkaDuplicateTopic`
        - routes unique events in `secondaryEvents` to `kafkaDenormSecondaryRouteTopic`
        - routes rest of the unique events to `kafkaDenormPrimaryRouteTopic`
        - also routes unique `AUDIT` events to `kafkaAuditRouteTopic` and `kafkaPrimaryRouteTopic`
        - also routes unique `ERROR` events to `kafkaErrorRouteTopic`
        - also routes unique `SHARE` events to `kafkaPrimaryRouteTopic` also, flattens them and routes flattened events
          again to `kafkaPrimaryRouteTopic`
        - also routes other unique events to `kafkaPrimaryRouteTopic`


#### De-Normalization `de-normalization`

Config variable             | Config key                            | Config value
----------------------------|---------------------------------------|---------------------------------
`telemetryInputTopic`       | kafka.input.telemetry.topic           | env.telemetry.unique
`summaryInputTopic`         | kafka.input.summary.topic             | env.telemetry.derived
`telemetryDenormOutputTopic`| kafka.telemetry.denorm.output.topic   | env.telemetry.denorm
`summaryDenormOutputTopic`  | kafka.summary.denorm.output.topic     | env.druid.events.summary
`summaryUniqueEventsTopic`  | kafka.summary.unique.events.topic     | env.telemetry.derived.unique
`failedTopic`               | kafka.output.failed.topic             | env.telemetry.failed
`duplicateTopic`            | kafka.output.duplicate.topic          | env.telemetry.duplicate
`eventsToskip`              | skip.events                           | INTERRUPT


`DenormalizationStreamTask`

This task performs de-normalization of event data (device, user, dial-code, content, location), and then routes events
to be validated for Druid Ingestion by [`druid-events-validator`](#druid-events-validator-druid-events-validator)

Source - `telemetryInputTopic`

Sink - `telemetryDenormOutputTopic`

Transformations -
- Reads from source stream, keys by `event.did()`, windows by `config.windowCount` then applies `DenormalizationWindowFunction`
    - `DenormalizationWindowFunction`
        - filter out events older than `config.ignorePeriodInMonths`
        - filter out events not suitable for de-normalization
        - de-normalize device, user, dial-code, content, location for rest of the events
        - route de-normalized events to `telemetryDenormOutputTopic`


`SummaryDenormalizationStreamTask`

This task performs de-duplication of summary data [coming from Spark?] then further performs de-normalization
device, user, dial-code, content, location in the summary data, which it routes to be Ingested by [Druid](#druid---olap-store)

Source - `summaryInputTopic`

Sink - `summaryUniqueEventsTopic`, `duplicateTopic`, `telemetryDenormOutputTopic`, `summaryDenormOutputTopic`

Transformations -
- Reads from source stream and applies `SummaryDeduplicationFunction`
    - `SummaryDeduplicationFunction`
        - routes duplicate summary events to `duplicateTopic`
        - routes unique summary events to `summaryUniqueEventsTopic`
        - applies `DenormalizationWindowFunction` to unique summary events after keying by `event.did()`, windowing by `config.windowCount`
    - `DenormalizationWindowFunction`
        - filter out events older than `config.ignorePeriodInMonths`
        - filter out events not suitable for de-normalization
        - de-normalize device, user, dial-code, content, location for rest of the events
        - route de-normalized events to `summaryDenormOutputTopic`


#### Druid Events Validator `druid-events-validator`

Config variable             | Config key                            | Config value
----------------------------|---------------------------------------|---------------------------------
`kafkaInputTopic`           | kafka.input.topic                     | env.telemetry.denorm
`kafkaTelemetryRouteTopic`  | kafka.output.telemetry.route.topic    | env.druid.events.telemetry
`kafkaSummaryRouteTopic`    | kafka.output.summary.route.topic      | env.druid.events.summary
`kafkaFailedTopic`          | kafka.output.failed.topic             | env.telemetry.failed
`kafkaDuplicateTopic`       | kafka.output.duplicate.topic          | env.telemetry.duplicate


`DruidValidatorStreamTask`

This task validates events for [Druid](#druid---olap-store) ingestion, summary and non-summary events are routed to respective kafka topics
after routing out invalid and duplicate events

Source - `kafkaInputTopic`

Sink - `kafkaTelemetryRouteTopic`, `kafkaSummaryRouteTopic`, `kafkaFailedTopic`, `kafkaDuplicateTopic`

Transformations -
- Reads from source stream then applies `DruidValidatorFunction`
    - `DruidValidatorFunction`
        - validates events for druid ingestion and routes invalid events to `kafkaFailedTopic`
        - checks if events are unique and routes duplicate events to `kafkaDuplicateTopic`
        - routes unique valid summary events to `kafkaSummaryRouteTopic`
        - routes unique valid non summary events to `kafkaTelemetryRouteTopic`


#### Assessment Aggregator `assessment-aggregator`

Config variable         | Config key                    | Config value
------------------------|-------------------------------|---------------------------------
`kafkaInputTopic`       | kafka.input.topic             | env.telemetry.assess
`kafkaFailedTopic`      | kafka.failed.topic            | env.telemetry.assess.failed
`kafkaCertIssueTopic`   | kafka.output.certissue.topic  | env.issue.certificate.request


`AssessmentAggregatorStreamTask`

This task aggregates assessment data saves it to cassandra and issues certificates

Source - `kafkaInputTopic`

Sink - `kafkaFailedTopic`, `kafkaCertIssueTopic`, cassandra

Transformations -
- Reads from source stream then applies `AssessmentAggregatorFunction`
    - `AssessmentAggregatorFunction`
        - checks validity of assess events and routes invalid/errored-out events to `kafkaFailedTopic`
        - performs aggregations on assess events and saves assessments to cassandra
        - creates certificate issue events and routes them to `kafkaCertIssueTopic`


#### Content Cache Updater `content-cache-updater`

Config variable     | Config key            | Config value
--------------------|-----------------------|---------------------------------
`inputTopic`        | kafka.input.topic     | env.learning.graph.events


`ContentCacheUpdaterStreamTask`

This task updates content cache and dial code properties in the redis cache

Source - `inputTopic`

Sink - redis

Transformations -
- Reads from source stream then applies `ContentUpdaterFunction`
    - `ContentUpdaterFunction`
        - updates redis content cache
        - if event has dial code properties, applies `DialCodeUpdaterFunction`
    - `DialCodeUpdaterFunction`
        - updates dial code properties in redis


#### Device Profile Updater `device-profile-updater`

Config variable         | Config key            | Config value
------------------------|-----------------------|---------------------------------
`kafkaInputTopic`       | kafka.input.topic     | env.events.deviceprofile


`DeviceProfileUpdaterStreamTask`

This task updates/inserts device profile to redis cache and postgres database

Source - `kafkaInputTopic`

Sink - redis, postgres

Transformations -
- Reads from source stream then applies `DeviceProfileUpdaterFunction`
    - `DeviceProfileUpdaterFunction`
        - ignores if there is no device id
        - adds the device to redis cache and postgres database


#### User Cache Updater `user-cache-updater`

Config variable     | Config key            | Config value
--------------------|-----------------------|---------------------------------
`inputTopic`        | kafka.input.topic     | env.telemetry.audit


`UserCacheUpdaterStreamTask`

This task creates or updates user data in redis cache, fetches user/location metadata from cassandra in case of update

Source - `inputTopic`

Sink - redis

Transformations -
- Reads from source stream then applies `UserCacheUpdaterFunction`
    - `UserCacheUpdaterFunction`
        - either, creates user data and updates redis cache
        - or, updates user data by fetching user metadata and location metadata from cassandra and then updates redis cache


#### User Cache Updater 2.0 `user-cache-updater-2.0`

Config variable     | Config key            | Config value
--------------------|-----------------------|---------------------------------
`inputTopic`        | kafka.input.topic     | env.telemetry.audit


`UserCacheUpdaterStreamTaskV2`

This task creates or updates user data in redis cache, fetches user/location metadata from cassandra in case of update

Source - `inputTopic`

Sink - redis

Transformations -
- Reads from source stream then applies `UserCacheUpdaterFunctionV2`
    - `UserCacheUpdaterFunctionV2`
        - creates user metadata or queries cassandra to get user metadata
        - updates user data in redis cache

<!---
#### Ingest Router `ingest-router`

`IngestRouterStreamTask`

```
// Something off here

kafka.input.topic = env.telemetry.ingestion
kafka.output.success.topic = env.telemetry.ingest

// Kafka Topics Configuration
val kafkaIngestInputTopic: String = config.getString(kafka.ingest.input.topic)
val kafkaIngestSuccessTopic: String = config.getString(kafka.ingest.output.success.topic)
val kafkaRawInputTopic: String = config.getString(kafka.raw.input.topic)
val kafkaRawSuccessTopic: String = config.getString(kafka.raw.output.success.topic)

```
--->

### Apache Spark - Batch Processing
[expand]

### Druid - OLAP Store

Data Store                  | Type      | Kafka Topic (env.)
----------------------------|-----------|------------------------
telemetry-events            | raw       | events.telemetry
telemetry-log-events        | raw       | events.log
summary-events              | raw       | events.summary
telemetry-feedback-events   | raw       | events.telemetry
tpd-hourly-rollup-syncts    | rollup    | druid.events.telemetry
telemetry-rollup-ets        | rollup    | events.telemetry
summary-rollup-syncts       | rollup    | events.summary
summary-rollup-ets          | rollup    | events.summary
summary-distinct-counts     | rollup    | events.summary
error-rollup-syncts         | rollup    | events.error


#### Superset

Visualize Druid data [expand]