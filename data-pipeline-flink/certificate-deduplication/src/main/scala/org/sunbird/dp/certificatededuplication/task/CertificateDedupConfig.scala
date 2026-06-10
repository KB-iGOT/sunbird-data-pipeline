package org.sunbird.dp.certificatededuplication.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.certificatededuplication.domain.Event
import org.sunbird.dp.core.job.BaseJobConfig
import org.sunbird.dp.core.sink.ClickhouseConfig

class CertificateDedupConfig(override val config: Config) extends BaseJobConfig(config, "CertificateDeduplicationJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")
  val kafkaUniqueTopic: String = config.getString("kafka.output.unique.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val downstreamOperatorsParallelism: Int = config.getInt("task.downstream.operators.parallelism")

  // Consumers
  val certificateDedupConsumer = "certificate-deduplication-consumer"

  // Producers
  val certificateDedupUniqueProducer = "certificate-deduplication-unique-producer"
  val certificateDedupDuplicateProducer = "certificate-deduplication-duplicate-producer"

  // Redis Configuration
  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")

  // Deduplication Configuration
  val dedupFlagName: String = "duplicate_certificate_event"


  //Clickhouse config
  val clickhouseUrl: String = config.getString("clickhouse.url")
  val clickhouseTable: String = config.getString("clickhouse.table.certificates")
  val clickhouseUsername: String = config.getString("clickhouse.username")
  val clickhousePassword: String = config.getString("clickhouse.password")
  val clickhouseBatchSize: Int = config.getInt("clickhouse.batch.size")

  val chConfig: ClickhouseConfig = ClickhouseConfig(
    url = clickhouseUrl,
    table = clickhouseTable,
    username = clickhouseUsername,
    password = clickhousePassword,
    batchSize = clickhouseBatchSize,
    flushIntervalMs = 5000L
  )

  //Metrics
  val totalEventsMetric: String = "certificate_total_events"
  val uniqueEventsMetric: String = "certificate_unique_events"
  val duplicateEventsMetric: String = "certificate_duplicate_events"
  val redisErrorMetric: String = "certificate_redis_errors"
  val clickhouseWriteMetric: String = "certificate_clickhouse_writes"

  //Output Tags
  val duplicateEventsOutputTag: OutputTag[Event] = OutputTag[Event]("duplicate-events")
  val failedEventsOutputTag: OutputTag[Event] = OutputTag[Event]("failed-events")
  val uniqueEventsOutputTag: OutputTag[Event] = OutputTag[Event]("unique-events")

  //Consumer
  val consumerName: String = "certificate-dedup-consumer"
  val clickhouseSinkName: String = "clickhouse-certificate-sink"
  val duplicateProducerName: String = "certificate-duplicate-producer"
}
