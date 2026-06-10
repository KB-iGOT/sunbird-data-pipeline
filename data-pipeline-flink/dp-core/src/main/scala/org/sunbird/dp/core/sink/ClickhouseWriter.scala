package org.sunbird.dp.core.sink
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import scala.collection.mutable.ListBuffer

class ClickhouseWriter(config: ClickhouseConfig, columns: Seq[String]){

  private var connection: Connection = _
  private var preparedStatement: PreparedStatement = _
  private val batchBuffer = new ListBuffer[util.Map[String, AnyRef]]()
  private val insertSQL = s"INSERT INTO ${config.fullTableName} (${columns.mkString(", ")}) VALUES (${columns.map(_ => "?").mkString(", ")})"

  def open(): Unit = {
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    connection = DriverManager.getConnection(config.url, config.username, config.password)
    preparedStatement = connection.prepareStatement(insertSQL)
  }

  def write(record: util.Map[String, AnyRef]): Unit = {
    batchBuffer += record
    if (batchBuffer.size >= config.batchSize) {
      flush()
    }
  }

  def flush(): Unit = {
    if (batchBuffer.isEmpty) return

      batchBuffer.foreach { record =>
        columns.zipWithIndex.foreach { case (col, idx) =>
          preparedStatement.setObject(idx + 1, record.get(col))
        }
        preparedStatement.addBatch()
      }
      preparedStatement.executeBatch()
      batchBuffer.clear()

  }

  def close(): Unit = {
    flush()
    if (preparedStatement != null) preparedStatement.close()
    if (connection != null) connection.close()
  }
}
