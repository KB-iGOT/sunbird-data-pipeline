package org.sunbird.dp.core.sink

case class ClickhouseConfig( url: String,
                             table: String,
                             username: String,
                             password: String,
                             batchSize: Int = 1000,
                             flushIntervalMs: Long = 5000)
{
  def fullTableName: String = table
}