package com.contiamo.spark.datasource.sap

import scala.util.chaining._
import java.util

import org.json4s._

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{
  DataSourceReader,
  InputPartition,
  InputPartitionReader,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/* TODO
  - SessionConfigSupport
  - partitioning columns
  - SessionReferenceProvider
 */

class SapDataSourceReader(options: DataSourceOptions)
    extends DataSourceReader
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {
  import SapDataSourceReader._

  protected val optionsMap: OptionsMap = options.asMap.asScala.toMap
  protected val filterPushDownEnabled: Boolean = optionsMap
    .get(SapDataSource.TABLE_FILTER_PUSHDOWN_ENABLED_KEY)
    .flatMap(s => Try(s.toBoolean).toOption)
    .getOrElse(true)

  private val partitionsInfo = SapDataSourceReader.createPartitions(optionsMap)

  override def readSchema(): StructType = partitionsInfo.schemaReader.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = partitionsInfo.partitions.asJava

  override def pruneColumns(requiredSchema: StructType): Unit = partitionsInfo.pruneColumns(requiredSchema)

  override def pushFilters(filters: Array[Filter]): Array[Filter] =
    if (filterPushDownEnabled) partitionsInfo.pushFilters(filters)
    else filters

  override def pushedFilters(): Array[Filter] = partitionsInfo.pushedFilters
}

object SapDataSourceReader {
  type OptionsMap = Map[String, String]
  type SapInputPartition = InputPartition[InternalRow]

  case class TablePartition(tableName: String,
                            requiredColumns: Option[StructType],
                            tableFilters: Seq[SapTableFilter],
                            jcoTableReadFunName: String,
                            jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapTablePartitionReader(this)
  }

  case class BapiPartition(funName: String,
                           bapiArgs: Map[String, JValue],
                           bapiOutputTable: Option[String],
                           bapiFlatten: Boolean,
                           requiredColumns: Option[StructType],
                           jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapBapiPartitionReader(this)
  }

  case class ListTablesPartition(tables: Seq[String], jcoTableReadFunName: String, jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapListTablesReader(this)
  }

  trait PartitionsInfo {
    def schemaReader: SapSchemaReader
    def partitions: Seq[SapInputPartition]

    def pruneColumns(requiredSchema: StructType): Unit = {}
    def pushFilters(filters: Array[Filter]): Array[Filter] = filters
    def pushedFilters: Array[Filter] = Array.empty
  }

  def extractJcoOptions(options: OptionsMap): Map[String, String] =
    options
      .filterKeys(_.startsWith("jco."))
      .map(identity) // scala bug workaround (https://github.com/scala/bug/issues/7005)

  def createPartitions(options: OptionsMap): PartitionsInfo = {
    import org.json4s.jackson.JsonMethods._
    import org.json4s.DefaultFormats

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jcoOptions = extractJcoOptions(options)
    val tableReadFun = options.getOrElse(SapDataSource.TABLE_READ_FUN_KEY, "RFC_READ_TABLE")

    def createTablePartitions: Option[PartitionsInfo] =
      options.get(SapDataSource.TABLE_KEY).map { tableName =>
        new PartitionsInfo {
          private var requiredColumns: Option[StructType] = None
          private var tableFilters: Array[SapTableFilter] = Array.empty

          private def partition =
            TablePartition(tableName, requiredColumns, tableFilters, tableReadFun, jcoOptions)
          def partitions = Seq(partition)
          def schemaReader = new SapTableSchemaReader(partition, noData = true)

          override def pruneColumns(requiredSchema: StructType): Unit = { requiredColumns = Some(requiredSchema) }
          override def pushFilters(filters: Array[Filter]): Array[Filter] = {
            val (pushed, rejected) = SapTableFilter(filters)
            tableFilters = pushed
            rejected
          }
          override def pushedFilters: Array[Filter] = tableFilters.map(_.filter)
        }
      }

    def createBapiPartitions: Option[PartitionsInfo] =
      options
        .get(SapDataSource.BAPI_KEY)
        .map {
          case bapiName =>
            val bapiArgsStr = options.getOrElse(SapDataSource.BAPI_ARGS_KEY, "{}")
            val bapiArgs = parse(bapiArgsStr) match {
              case JObject(args) => args.toMap
              case _ =>
                throw new InvalidConfigurationException(s"${SapDataSource.BAPI_ARGS_KEY} must contain a JSON object")
            }

            val bapiOutput = options.get(SapDataSource.BAPI_OUTPUT_TABLE_KEY)
            val bapiFlatten = options.getOrElse(SapDataSource.BAPI_FLATTEN_KEY, "") == "true"

            new PartitionsInfo {
              private var requiredColumns: Option[StructType] = None
              private def partition =
                BapiPartition(bapiName, bapiArgs, bapiOutput, bapiFlatten, requiredColumns, jcoOptions)
              def partitions = Seq(partition)
              def schemaReader = new SapBapiPartitionReader(partition, true)
              override def pruneColumns(requiredSchema: StructType): Unit = { requiredColumns = Some(requiredSchema) }
            }
        }

    def createListTablesPartitions: Option[PartitionsInfo] =
      options
        .get(SapDataSource.LIST_TABLES_KEY)
        .flatMap(parse(_).extractOpt[Array[String]])
        .map { tables =>
          new PartitionsInfo {
            private val partition = ListTablesPartition(tables, tableReadFun, jcoOptions)
            val partitions = Seq(partition)
            def schemaReader = new SapListTablesReader(partition)
          }
        }

    createTablePartitions
      .orElse(createBapiPartitions)
      .orElse(createListTablesPartitions)
      .getOrElse(throw new InvalidConfigurationException("no TABLE, BAPI or metadata configuration was provided"))
  }
}
