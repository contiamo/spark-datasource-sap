package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class SapDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader =
    new SapDataSourceReader(options)
}

object SapDataSource {
  val TABLE_KEY = DataSourceOptions.TABLE_KEY
  val BAPI_KEY = "bapi-name"
  val BAPI_ARGS_KEY = "bapi-args"
  val BAPI_OUTPUT_KEY = "bapi-output"
}
