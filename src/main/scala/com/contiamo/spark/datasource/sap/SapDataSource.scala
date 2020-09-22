package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class SapDataSource extends SimpleTableProvider {
  override def getTable(optionsIn: CaseInsensitiveStringMap): Table = {
    val options = optionsIn.asCaseSensitiveMap().asScala.toMap

    SapTableReader(options)
      .orElse(SapBapiReader(options))
      .orElse(SapListTablesReader(options))
      .orElse(SapListBapisReader(options))
      .getOrElse(throw new InvalidConfigurationException("no TABLE, BAPI or metadata configuration was provided"))
  }
}

object SapDataSource {
  def extractJcoOptions(options: OptionsMap): Map[String, String] =
    options
      .filterKeys(_.startsWith("jco."))
      .map(identity) // scala bug workaround (https://github.com/scala/bug/issues/7005)

  val TABLE_KEY = "table"
  val TABLE_READ_FUN_KEY = "table-read-function"
  val TABLE_FILTER_PUSHDOWN_ENABLED_KEY = "table-read-filter-pushdown"
  val LIST_TABLES_KEY = "list-tables-like"
  val LIST_BAPIS_KEY = "list-bapis-like"
  val BAPI_KEY = "bapi"
  val BAPI_ARGS_KEY = "bapi-args"
  val BAPI_OUTPUT_TABLE_KEY = "bapi-output-table"
  val BAPI_FLATTEN_KEY = "bapi-output-flatten"
}
