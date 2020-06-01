package com.contiamo.spark.datasource.sap

import scala.util.chaining._
import java.util

import org.json4s._

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{
  DataSourceReader,
  InputPartition,
  InputPartitionReader,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/* TODO
  - with SupportsPushDownFilters
  - partitioning columns
  - SessionReferenceProvider
 */

class SapDataSourceReader(options: DataSourceOptions) extends DataSourceReader with SupportsPushDownRequiredColumns {
  protected val optionsMap: SapDataSourceReader.OptionsMap = options.asMap().asScala.toMap

  private var requiredColumns: Option[StructType] = None
  private var partitionsInfo = SapDataSourceReader.createPartitions(optionsMap, requiredColumns)

  override def readSchema(): StructType = partitionsInfo.schemaReader.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = partitionsInfo.partitions.asJava

  override def pruneColumns(requiredSchema: StructType): Unit = {
    requiredColumns = Option(requiredSchema)
    partitionsInfo = SapDataSourceReader.createPartitions(optionsMap, requiredColumns)
  }
}

object SapDataSourceReader {
  type OptionsMap = Map[String, String]
  type SapInputPartition = InputPartition[InternalRow]

  case class TablePartition(tableName: String, requiredColumns: Option[StructType], jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapTablePartitionReader(this)
  }

  case class BapiPartition(funName: String,
                           bapiArgs: Map[String, JValue],
                           bapiOutput: String,
                           bapiFlatten: Boolean,
                           requiredColumns: Option[StructType],
                           jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapBapiPartitionReader(this)
  }

  case class PartitionsInfo(schemaReader: SapSchemaReader, partitions: Seq[SapInputPartition])

  def extractJcoOptions(options: OptionsMap): Map[String, String] = {
    val jcoPrefixes = Seq("client", "destination")
    options
      .filterKeys(opt => jcoPrefixes.exists(prefix => opt.startsWith(prefix)))
      .map {
        case (k, v) => (s"jco.$k", v)
      }
      .toMap
  }

  protected def createTablePartitions(options: OptionsMap,
                                      requiredColumns: Option[StructType]): Option[PartitionsInfo] = {
    val jcoOptions = extractJcoOptions(options)

    options.get(SapDataSource.TABLE_KEY).map { tableName =>
      val partition = TablePartition(tableName, requiredColumns, jcoOptions)
      val schemaReader = new SapTableSchemaReader(partition)

      PartitionsInfo(schemaReader, Seq(partition))
    }
  }

  protected def createBapiPartitions(options: OptionsMap,
                                     requiredColumns: Option[StructType]): Option[PartitionsInfo] = {
    val jcoOptions = extractJcoOptions(options)

    import org.json4s.jackson.JsonMethods._

    options
      .get(SapDataSource.BAPI_KEY)
      .flatMap { bapiName =>
        val bapiArgsStr = options.getOrElse(SapDataSource.BAPI_ARGS_KEY, "{}")
        parse(bapiArgsStr) match {
          case JObject(args) =>
            Some((bapiName, args.toMap))
          case _ => None
        }
      }
      .map {
        case (bapiName, bapiArgs) =>
          val bapiOutput = options.getOrElse(SapDataSource.BAPI_OUTPUT_KEY, "")
          val bapiFlatten = options.getOrElse(SapDataSource.BAPI_FLATTEN_KEY, "") == "true"
          val partition = BapiPartition(bapiName, bapiArgs, bapiOutput, bapiFlatten, requiredColumns, jcoOptions)
          val schemaReader = new SapBapiPartitionReader(partition, true)

          PartitionsInfo(schemaReader, Seq(partition))
      }
  }

  def createPartitions(options: OptionsMap, requiredColumns: Option[StructType]): PartitionsInfo =
    createTablePartitions(options, requiredColumns).orElse(createBapiPartitions(options, requiredColumns)).get
}
