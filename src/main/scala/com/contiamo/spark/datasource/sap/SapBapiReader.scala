package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapBapiReader.Partition
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

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

  override def pruneColumns(requiredSchema: StructType): Unit = { requiredColumns = Some(requiredSchema) }

  override def name(): String = bapiName

  override def build(): Scan = new SapScan[Partition] {
    override val partition =
      Partition(bapiName, bapiArgs, bapiOutput, bapiFlatten, requiredColumns, jcoOptions)

    override def readSchema() =
      new SapBapiPartitionReader(partition, schemaOnly = true).schema
  }
}

object SapBapiReader {
  case class Partition(funName: String,
                       bapiArgs: Map[String, JValue],
                       bapiOutputTable: Option[String],
                       bapiFlatten: Boolean,
                       requiredColumns: Option[StructType],
                       jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader() = new SapBapiPartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options.get(SapDataSource.BAPI_KEY).map(new SapBapiReader(_, options))
}
