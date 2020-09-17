package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapTableReader.Partition
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.util.Try

class SapTableReader(tableName: String, override val options: OptionsMap)
    extends SapDataSourceBaseReader
    with SupportsPushDownFilters {
  private var requiredColumns: Option[StructType] = None
  private var tableFilters: SapTableFilters = SapTableFilters.empty

  private val filterPushDownEnabled: Boolean = options
    .get(SapDataSource.TABLE_FILTER_PUSHDOWN_ENABLED_KEY)
    .flatMap(s => Try(s.toBoolean).toOption)
    .getOrElse(true)

  override def pruneColumns(requiredSchema: StructType): Unit = { requiredColumns = Some(requiredSchema) }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (filterPushDownEnabled) {
      tableFilters = SapTableFilters(filters, schema())
    }
    filters
  }
  override def pushedFilters: Array[Filter] = tableFilters.pushed

  override def name(): String = tableName

  override def build(): Scan =
    new SapScan[Partition] {
      override val partition =
        Partition(tableName, requiredColumns, tableFilters.whereClauseLines, tableReadFun, jcoOptions)

      override def readSchema() =
        new SapTableSchemaReader(partition, noData = true).schema
    }
}

object SapTableReader {
  case class Partition(tableName: String,
                       requiredColumns: Option[StructType],
                       whereClauseLines: Seq[String],
                       jcoTableReadFunName: String,
                       jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader() = new SapTablePartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options.get(SapDataSource.TABLE_KEY).map { tableName =>
      new SapTableReader(tableName, options)
    }
}
