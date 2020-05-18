package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class SapDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader =
    new SapDataSourceReader(options)
}
