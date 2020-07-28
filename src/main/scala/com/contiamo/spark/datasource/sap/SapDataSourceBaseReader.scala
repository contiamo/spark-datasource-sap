package com.contiamo.spark.datasource.sap

import java.util

import com.contiamo.spark.datasource.sap.SapDataSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

trait SapDataSourceBaseReader
    extends DataSourceReader
    with SupportsPushDownRequiredColumns {

  def options: OptionsMap

  val jcoOptions = extractJcoOptions(options)
  val tableReadFun = options.getOrElse(SapDataSource.TABLE_READ_FUN_KEY, "RFC_READ_TABLE")

  def schemaReader: SapSchemaReader
  def partitions: Seq[SapInputPartition]

  override def readSchema(): StructType = schemaReader.schema
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = partitions.asJava

  override def pruneColumns(requiredSchema: StructType): Unit = {}
}
