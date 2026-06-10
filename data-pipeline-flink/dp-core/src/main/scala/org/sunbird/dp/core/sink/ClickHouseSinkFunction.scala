package org.sunbird.dp.core.sink
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

import java.util
import java.sql.Connection

class ClickHouseSinkFunction(
                              config: ClickhouseConfig,
                              columns: Seq[String]
                            ) extends RichSinkFunction[util.Map[String, AnyRef]] {

  @transient private var writer: ClickhouseWriter = _
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
    writer = new ClickhouseWriter(config, columns)
    writer.open()
    logger.info("ClickHouse sink initialized")
  }

  override def invoke(value: util.Map[String, AnyRef]): Unit = {
    writer.write(value)
  }

  override def close(): Unit = {
    writer.flush()
    writer.close()
  }
}