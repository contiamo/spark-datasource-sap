package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapDataSourceTableReader.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartitionReader, SupportsPushDownFilters}
import org.apache.spark.sql.types.StructType

import scala.util.Try

class SapDataSourceTableReader(tableName: String, override val options: OptionsMap)
    extends SapDataSourceBaseReader
    with SupportsPushDownFilters {
  private var requiredColumns: Option[StructType] = None
  private var tableFilters: SapTableFilters = SapTableFilters.empty

  private val filterPushDownEnabled: Boolean = options
    .get(SapDataSource.TABLE_FILTER_PUSHDOWN_ENABLED_KEY)
    .flatMap(s => Try(s.toBoolean).toOption)
    .getOrElse(true)

  private def partition =
    Partition(tableName, requiredColumns, tableFilters.whereClauseLines, tableReadFun, jcoOptions)

  override def partitions = Seq(partition)
  override def schemaReader = new SapTableSchemaReader(partition, noData = true)

  override def pruneColumns(requiredSchema: StructType): Unit = { requiredColumns = Some(requiredSchema) }
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (filterPushDownEnabled) {
      tableFilters = SapTableFilters(filters, schemaReader.schema)
    }
    filters
  }
  override def pushedFilters: Array[Filter] = tableFilters.pushed
}

object SapDataSourceTableReader {
  case class Partition(tableName: String,
                       requiredColumns: Option[StructType],
                       whereClauseLines: Seq[String],
                       jcoTableReadFunName: String,
                       jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] =
      new SapTablePartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options.get(SapDataSource.TABLE_KEY).map { tableName =>
      new SapDataSourceTableReader(tableName, options)
    }
}
