package com.contiamo.spark.datasource.sap

import scala.util.chaining._
import java.util

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/* TODO
  - with SupportsPushDownRequiredColumns
  - with SupportsPushDownFilters
  - arbitrary BAPI support
  - partitioning columns
  - SessionReferenceProvider
 */

class SapDataSourceReader(options: DataSourceOptions) extends DataSourceReader {
  private val (schemaReader, partitions) = SapDataSourceReader.createPartitions(options)

  schemaReader.ping()

  override def readSchema(): StructType = schemaReader.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = partitions.asJava
}

object SapDataSourceReader {
  type SapInputPartition = InputPartition[InternalRow]

  case class TablePartition(tableName: String, jcoOptions: Map[String, String]) extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapTablePartitionReader(this)
  }

  def extractJcoOptions(options: DataSourceOptions): Map[String, String] = {
    val jcoPrefixes = Seq("client", "destination")
    options
      .asMap()
      .asScala
      .filterKeys(opt => jcoPrefixes.exists(prefix => opt.startsWith(prefix)))
      .map {
        case (k, v) => (s"jco.$k", v)
      }
      .toMap
  }

  def createPartitions(options: DataSourceOptions): (SapSchemaReader, Seq[SapInputPartition]) = {
    val jcoOptions = extractJcoOptions(options)

    val tableName = options.get(DataSourceOptions.TABLE_KEY).orElse("")
    val partition = TablePartition(tableName, jcoOptions)
    val schemaReader = new SapTablePartitionReader(partition, true)

    (schemaReader, Seq(partition))
  }
}
