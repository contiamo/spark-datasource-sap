package com.contiamo.spark.datasource.sap

import java.util

import com.contiamo.spark.datasource.sap.SapDataSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

trait SapDataSourceBaseReader extends SupportsRead with ScanBuilder with SupportsPushDownRequiredColumns {

  def options: OptionsMap

  val jcoOptions = extractJcoOptions(options)
  val tableReadFun = options.getOrElse(SapDataSource.TABLE_READ_FUN_KEY, "RFC_READ_TABLE")

  override def pruneColumns(requiredSchema: StructType): Unit = {}

  override def schema(): StructType = build().readSchema()

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = this
}
