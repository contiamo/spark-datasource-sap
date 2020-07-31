package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapListBapisReader.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

class SapListBapisReader(bapis: Seq[String], override val options: OptionsMap) extends SapDataSourceBaseReader {
  private val bapiFlatten = options.getOrElse(SapDataSource.BAPI_FLATTEN_KEY, "") == "true"

  private val partition = Partition(bapis, bapiFlatten, jcoOptions)
  override val partitions = Seq(partition)
  override def schemaReader = new SapListBapisPartitionReader(partition)
}

object SapListBapisReader {
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  implicit val formats: DefaultFormats.type = DefaultFormats

  case class Partition(bapis: Seq[String], bapiFlatten: Boolean, jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] =
      new SapListBapisPartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options
      .get(SapDataSource.LIST_BAPIS_KEY)
      .flatMap(parse(_).extractOpt[Array[String]])
      .map(new SapListBapisReader(_, options))
}
