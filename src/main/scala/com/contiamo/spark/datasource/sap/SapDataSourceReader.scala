package com.contiamo.spark.datasource.sap

import scala.util.chaining._
import java.util

import org.json4s._

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
  protected val optionsMap: SapDataSourceReader.OptionsMap = options.asMap().asScala.toMap

  private val (schemaReader, partitions) = SapDataSourceReader.createPartitions(optionsMap)

  schemaReader.ping()

  override def readSchema(): StructType = schemaReader.schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = partitions.asJava
}

object SapDataSourceReader {
  type OptionsMap = Map[String, String]
  type SapInputPartition = InputPartition[InternalRow]

  case class TablePartition(tableName: String, jcoOptions: Map[String, String]) extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapTablePartitionReader(this)
  }

  case class BapiPartition(funName: String,
                           bapiArgs: Map[String, JValue],
                           bapiOutput: String,
                           jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapBapiPartitionReader(this)
  }

  def extractJcoOptions(options: OptionsMap): Map[String, String] = {
    val jcoPrefixes = Seq("client", "destination")
    options
      .filterKeys(opt => jcoPrefixes.exists(prefix => opt.startsWith(prefix)))
      .map {
        case (k, v) => (s"jco.$k", v)
      }
      .toMap
  }

  protected def createTablePartitions(options: OptionsMap): Option[(SapSchemaReader, Seq[SapInputPartition])] = {
    val jcoOptions = extractJcoOptions(options)

    options.get(SapDataSource.TABLE_KEY).map { tableName =>
      val partition = TablePartition(tableName, jcoOptions)
      val schemaReader = new SapTablePartitionReader(partition, true)

      (schemaReader, Seq(partition))
    }
  }

  protected def createBapiPartitions(options: OptionsMap): Option[(SapSchemaReader, Seq[SapInputPartition])] = {
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
          val partition = BapiPartition(bapiName, bapiArgs, bapiOutput, jcoOptions)
          val schemaReader = new SapBapiPartitionReader(partition, true)

          (schemaReader, Seq(partition))
      }
  }

  def createPartitions(options: OptionsMap): (SapSchemaReader, Seq[SapInputPartition]) =
    createTablePartitions(options).orElse(createBapiPartitions(options)).get
}
