package com.contiamo.spark.datasource.sap

import com.sap.conn.jco.{JCoDestination, JCoDestinationManager}
import org.apache.spark.sql.types.StructType

trait SapSchemaReader {
  def jcoOptions: Map[String, String]

  private val destKey = SapSparkDestinationDataProvider.register(jcoOptions)
  protected val dest: JCoDestination = JCoDestinationManager.getDestination(destKey)

  def schema: StructType

  def ping(): Unit = dest.ping()
}
