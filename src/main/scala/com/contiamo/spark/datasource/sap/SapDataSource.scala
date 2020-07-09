package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class SapDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader =
    new SapDataSourceReader(options)
}

object SapDataSource {
  val TABLE_KEY = DataSourceOptions.TABLE_KEY
  val TABLE_READ_FUN_KEY = "table-read-function"
  val LIST_TABLES_KEY = "list-tables-like"
  val BAPI_KEY = "bapi"
  val BAPI_ARGS_KEY = "bapi-args"
  val BAPI_OUTPUT_TABLE_KEY = "bapi-output-table"
  val BAPI_FLATTEN_KEY = "bapi-output-flatten"
}
