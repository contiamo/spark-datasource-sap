package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapBapiReader.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.json4s.{DefaultFormats, _}
import org.json4s.jackson.JsonMethods._

class SapBapiReader(bapiName: String, override val options: OptionsMap) extends SapDataSourceBaseReader {
  implicit val formats: DefaultFormats.type = DefaultFormats

  private val bapiArgsStr = options.getOrElse(SapDataSource.BAPI_ARGS_KEY, "{}")
  private val bapiArgs = parse(bapiArgsStr) match {
    case JObject(args) => args.toMap
    case _ =>
      throw new InvalidConfigurationException(s"${SapDataSource.BAPI_ARGS_KEY} must contain a JSON object")
  }

  private val bapiOutput = options.get(SapDataSource.BAPI_OUTPUT_TABLE_KEY)
  private val bapiFlatten = options.getOrElse(SapDataSource.BAPI_FLATTEN_KEY, "") == "true"

  private var requiredColumns: Option[StructType] = None
  private def partition =
    Partition(bapiName, bapiArgs, bapiOutput, bapiFlatten, requiredColumns, jcoOptions)

  override def partitions = Seq(partition)
  override def schemaReader = new SapBapiPartitionReader(partition, true)
  override def pruneColumns(requiredSchema: StructType): Unit = { requiredColumns = Some(requiredSchema) }
}

object SapBapiReader {
  case class Partition(funName: String,
                       bapiArgs: Map[String, JValue],
                       bapiOutputTable: Option[String],
                       bapiFlatten: Boolean,
                       requiredColumns: Option[StructType],
                       jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapBapiPartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options.get(SapDataSource.BAPI_KEY).map(new SapBapiReader(_, options))
}
