package org.sunbird.dp.certificatededuplication.task

import org.sunbird.dp.certificatededuplication.domain.Event
import org.sunbird.dp.certificatededuplication.functions.CertificateDedupFunction
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.functions.MapFunction
import java.util
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import java.io.File
import org.sunbird.dp.core.job.FlinkKafkaConnector
import org.sunbird.dp.core.sink.ClickHouseSinkFunction
import org.sunbird.dp.core.util.FlinkUtil


class CertificateDeDuplicationStreamTask(config: CertificateDedupConfig, kafkaConnector: FlinkKafkaConnector) {

  def process(): Unit = {
//    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(config.parallelism)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    val kafkaConsumer = kafkaConnector.kafkaEventSource[Event](config.kafkaInputTopic)

    val dedupStream = env.addSource(kafkaConsumer).name(config.certificateDedupConsumer)
      .uid(config.certificateDedupConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
      .process(new CertificateDedupFunction(config))
      .name("certificate-deduplication")

    //    dedupStream.addSink(new ClickHouseSinkFunction(config))
    //      .name(config.clickhouseSinkName)
    //      .uid(config.clickhouseSinkName)
    //      .setParallelism(config.sinkParallelism)

    val uniqueStream = dedupStream.getSideOutput(config.uniqueEventsOutputTag)
    val duplicateStream = dedupStream.getSideOutput(config.duplicateEventsOutputTag)

    // Map unique events to filtered maps once and reuse the mapped stream
    val filteredUnique = uniqueStream.map(new MapFunction[Event, util.Map[String, AnyRef]] {
      override def map(event: Event): util.Map[String, AnyRef] = event.toFilteredMap
    })

    // send to Kafka unique topic
    filteredUnique.addSink(kafkaConnector.kafkaMapSink(config.kafkaUniqueTopic))
      .name(config.certificateDedupUniqueProducer).uid(config.certificateDedupUniqueProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    // also write to ClickHouse (reuse same filtered map)
    filteredUnique.addSink(new ClickHouseSinkFunction(
      config.chConfig,
      Seq("mid", "primaryCategory", "orgId", "userId", "courseName", "courseCategory", "issuedDate", "batchId", "courseId")
    ))

    // Duplicate stream -> Kafka duplicate topic
    duplicateStream.map(new MapFunction[Event, util.Map[String, AnyRef]] {
      override def map(event: Event): util.Map[String, AnyRef] = event.toFilteredMap
    })
      .addSink(kafkaConnector.kafkaMapSink(config.kafkaDuplicateTopic))
      .name(config.certificateDedupDuplicateProducer).uid(config.certificateDedupDuplicateProducer)
      .setParallelism(config.downstreamOperatorsParallelism)

    env.execute(config.jobName)
  }
}

object CertificateDedupJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      logger.info("Starting Certificate Deduplication Flink Job")

      // Load configuration
      val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
      val config = configFilePath.map {
        path => ConfigFactory.parseFile(new File(path)).resolve()
      }.getOrElse(ConfigFactory.load("certificate-dedup.conf").withFallback(ConfigFactory.systemEnvironment()))
      val certificateDedupConfig = new CertificateDedupConfig(config)
      logger.info(s"Configuration loaded:\n$config")
      val kafkaUtil = new FlinkKafkaConnector(certificateDedupConfig)

      // Process the stream
      val deduplicationTask = new CertificateDeDuplicationStreamTask(certificateDedupConfig, kafkaUtil)
      deduplicationTask.process()
    } catch {
      case ex: Exception =>
        logger.error("Error in Certificate Deduplication Flink Job", ex)
        throw ex
    }
  }
}


