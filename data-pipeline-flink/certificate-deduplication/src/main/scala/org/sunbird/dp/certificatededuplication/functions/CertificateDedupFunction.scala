package org.sunbird.dp.certificatededuplication.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory

import org.sunbird.dp.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}

import org.sunbird.dp.certificatededuplication.domain.Event
import org.sunbird.dp.certificatededuplication.task.CertificateDedupConfig

class CertificateDedupFunction(config: CertificateDedupConfig,
                                @transient var dedupEngine: DedupEngine = null
                              )(implicit val eventTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger =
    LoggerFactory.getLogger(classOf[CertificateDedupFunction])

  override def metricsList(): List[String] = {
    List(
      config.totalEventsMetric,
      config.uniqueEventsMetric,
      config.duplicateEventsMetric,
      config.redisErrorMetric
    ) ::: deduplicationMetrics
  }

  override def open(parameters: Configuration): Unit = {

    super.open(parameters)

    if (dedupEngine == null) {

      val redisConnect =
        new RedisConnect(
          config.redisHost,
          config.redisPort,
          config
        )

      dedupEngine =
        new DedupEngine(
          redisConnect,
          config.dedupStore,
          config.cacheExpirySeconds
        )
    }

    logger.info("CertificateDedupFunction initialized")
  }

  override def close(): Unit = {

    super.close()

    if (dedupEngine != null) {
      dedupEngine.closeConnectionPool()
    }
  }

  override def processElement(event: Event,context: ProcessFunction[Event, Event]#Context,metrics: Metrics): Unit = {

    metrics.incCounter(config.totalEventsMetric)

    val isUnique = deDuplicate[Event, Event](event.dedupKey,event,context,config.duplicateEventsOutputTag,flagName = config.dedupFlagName
      )(dedupEngine, metrics)

    if (isUnique) {

      metrics.incCounter(config.uniqueEventsMetric)

      // forward unique event downstream
      context.output(config.uniqueEventsOutputTag, event)

    } else {

      metrics.incCounter(config.duplicateEventsMetric)

      logger.debug(s"Duplicate certificate event skipped: ${event.mid}")
    }
  }
}