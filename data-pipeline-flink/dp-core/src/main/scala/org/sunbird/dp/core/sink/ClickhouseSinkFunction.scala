package org.sunbird.dp.core.sink
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

import java.util
import java.sql.Connection

class ClickHouseSinkFunction(
                              config: ClickhouseConfig,
                              columns: Seq[String]
                            ) extends RichSinkFunction[util.Map[String, AnyRef]] {

  @transient private var writer: ClickhouseWriter = _
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def open(parameters: Configuration): Unit = {
    // Do not eagerly open ClickhouseWriter here. Defer creation until first element arrives.
    logger.info("ClickHouse sink initialized (writer will be opened on first invoke)")
  }

  override def invoke(value: util.Map[String, AnyRef], context: SinkFunction.Context): Unit = {
    if (writer == null) {
      try {
        writer = new ClickhouseWriter(config, columns)
        writer.open()
        logger.info("ClickhouseWriter opened on-demand")
      } catch {
        case ex: Exception =>
          logger.error("Failed to open ClickhouseWriter on first invoke", ex)
          throw ex
      }
    }
    writer.write(value)
  }

  override def close(): Unit = {
    if (writer != null) {
      try writer.flush() catch { case ex: Exception => logger.warn("Error flushing writer during close", ex) }
      try writer.close() catch { case ex: Exception => logger.warn("Error closing writer during close", ex) }
    }
    super.close()
  }
}