package org.sunbird.dp.preprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.preprocessor.domain.Event
import org.sunbird.dp.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.dp.preprocessor.util.TelemetryValidator

class PipelinePreprocessorFunction(config: PipelinePreprocessorConfig,
                                   @transient var telemetryValidator: TelemetryValidator = null,
                                   @transient var dedupEngine: DedupEngine = null)
                                   (implicit val eventTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[PipelinePreprocessorFunction])

  override def metricsList(): List[String] = {
    List(config.validationFailureMetricsCount,
      config.validationSuccessMetricsCount,
      config.duplicationSkippedEventMetricsCount,
      config.primaryRouterMetricCount,
      config.logEventsRouterMetricsCount,
      config.errorEventsRouterMetricsCount,
      config.auditEventRouterMetricCount,
      config.denormSecondaryEventsRouterMetricsCount,
      config.denormPrimaryEventsRouterMetricsCount
    ) ::: deduplicationMetrics
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
    }

    if (telemetryValidator == null) {
      telemetryValidator = new TelemetryValidator(config)
    }
  }

  override def close(): Unit = {
    super.close()
    dedupEngine.closeConnectionPool()
  }

  def isDuplicateCheckRequired(producerId: String): Boolean = {
    // config.includedProducersForDedup.contains(producerId)
    true
  }

  def addHubField(event: Event): Unit = {
    val clientEventTypes = Set("IMPRESSION", "INTERACT", "START", "END")
    val envHubMap = Map("learn" -> "learn", "course" -> "learn", "discuss" -> "discuss", "network" -> "network",
      "careers" -> "careers", "competency" -> "competency", "events" -> "events")
    if (clientEventTypes.contains(event.eid().toUpperCase())) {
      val hub = envHubMap.getOrElse(event.env.toLowerCase(), "other")
      event.updateHub(hub)
    }
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {

    val isValid = telemetryValidator.validate(event, context, metrics)

    if (isValid) {
      if (event.eid().equalsIgnoreCase("LOG")) {
        if (!config.skipLogEvents) {
          context.output(config.logEventsOutputTag, event)
          metrics.incCounter(metric = config.logEventsRouterMetricsCount)
        }
      } else if (event.eid().equalsIgnoreCase("CB_AUDIT") || event.eid().equalsIgnoreCase("SHARE")) {
        // drop — no longer processed
      }
      else {
        val isUnique = if (isDuplicateCheckRequired(event.producerId())) {
          deDuplicate[Event, Event](event.mid(), event, context, config.duplicateEventsOutputTag,
            flagName = config.DEDUP_FLAG_NAME)(dedupEngine, metrics)
        } else {
          event.markSkipped(config.DEDUP_SKIP_FLAG_NAME)
          metrics.incCounter(uniqueEventMetricCount)
          true
        }

        if (isUnique) {

          // add hub info
          addHubField(event)

          if("ERROR".equalsIgnoreCase(event.eid())) {
            metrics.incCounter(metric = config.errorEventsRouterMetricsCount)
          } else if (config.isDenormEnabled && config.secondaryEvents.contains(event.eid())) {
            context.output(config.denormSecondaryEventsRouteOutputTag, event)
            metrics.incCounter(metric = config.denormSecondaryEventsRouterMetricsCount)
          } else if (config.isDenormEnabled) {
            context.output(config.denormPrimaryEventsRouteOutputTag, event)
            metrics.incCounter(metric = config.denormPrimaryEventsRouterMetricsCount)
          }
          event.eid() match {
            case "AUDIT" =>
              context.output(config.auditRouteEventsOutputTag, event)
              metrics.incCounter(metric = config.auditEventRouterMetricCount)
              metrics.incCounter(metric = config.primaryRouterMetricCount) // Since we are are sinking the AUDIT Event into primary router topic
            case "ERROR" =>
              context.output(config.errorEventOutputTag, event)
            case _ => context.output(config.primaryRouteEventsOutputTag, event)
              metrics.incCounter(metric = config.primaryRouterMetricCount)
          }
        }
      }
    }

  }

}
