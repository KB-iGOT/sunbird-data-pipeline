package org.sunbird.dp.core.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

class ClickhouseWriter(config: ClickhouseConfig, columns: Seq[String]){

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var connection: Connection = _
  private var preparedStatement: PreparedStatement = _
  private val batchBuffer = new ListBuffer[util.Map[String, AnyRef]]()
  private val insertSQL = s"INSERT INTO ${config.fullTableName} (${columns.mkString(", ")}) VALUES (${columns.map(_ => "?").mkString(", ")})"

  def open(): Unit = {
    logger.info(s"Opening ClickhouseWriter. insertSQL=$insertSQL, batchSize=${config.batchSize}")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    connection = DriverManager.getConnection(config.url, config.username, config.password)
    // Ensure explicit commit control
    connection.setAutoCommit(false)
    preparedStatement = connection.prepareStatement(insertSQL)
    logger.info("ClickhouseWriter opened and PreparedStatement created")
  }

  def write(record: util.Map[String, AnyRef]): Unit = {
    try {
      // Log minimal identifying info if available
      val mid = Option(record.get("mid")).map(_.toString).getOrElse("-")
      logger.debug(s"Appending record to batch (mid=$mid). currentBatchSize=${batchBuffer.size + 1}")
      batchBuffer += record
      if (batchBuffer.size >= config.batchSize) {
        logger.info(s"Batch size reached (${batchBuffer.size}). Flushing to ClickHouse")
        flush()
      }
    } catch {
      case ex: Exception =>
        logger.error("Error while writing record to batchBuffer", ex)
        throw ex
    }
  }

  def flush(): Unit = {
    if (batchBuffer.isEmpty) {
      logger.debug("Flush called but batchBuffer is empty")
      return
    }

    logger.info(s"Flushing ${batchBuffer.size} records to ClickHouse")
    try {
      batchBuffer.foreach { record =>
        columns.zipWithIndex.foreach { case (col, idx) =>
          preparedStatement.setObject(idx + 1, record.get(col))
        }
        preparedStatement.addBatch()
      }

      val counts = preparedStatement.executeBatch()
      connection.commit()
      val updated = if (counts != null) counts.sum else -1
      logger.info(s"Flush complete. executeBatch returned ${if (counts != null) counts.length else 0} entries, sum=$updated")
    } catch {
      case ex: Exception =>
        logger.error("Exception during flush to ClickHouse", ex)
        throw ex
    } finally {
      batchBuffer.clear()
    }
  }

  def close(): Unit = {
    logger.info("Closing ClickhouseWriter: flushing remaining records and closing resources")
    try {
      flush()
    } catch {
      case ex: Exception => logger.error("Error while flushing on close", ex)
    }
    try if (preparedStatement != null) preparedStatement.close() catch { case ex: Exception => logger.warn("Error closing PreparedStatement", ex) }
    try if (connection != null) connection.close() catch { case ex: Exception => logger.warn("Error closing Connection", ex) }
    logger.info("ClickhouseWriter closed")
  }
}
