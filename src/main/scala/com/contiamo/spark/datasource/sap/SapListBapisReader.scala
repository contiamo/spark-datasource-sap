package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapListBapisReader.Partition
import org.apache.spark.sql.connector.read.Scan

class SapListBapisReader(bapis: Seq[String], override val options: OptionsMap) extends SapDataSourceBaseReader {
  private val bapiFlatten = options.getOrElse(SapDataSource.BAPI_FLATTEN_KEY, "") == "true"

  override def name(): String = SapDataSource.LIST_BAPIS_KEY

  override def build(): Scan = new SapScan[Partition] {
    override val partition = Partition(bapis, bapiFlatten, jcoOptions)
    override def readSchema() = SapListBapisPartitionReader.schema
  }
}

object SapListBapisReader {
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  implicit val formats: DefaultFormats.type = DefaultFormats

  case class Partition(bapis: Seq[String], bapiFlatten: Boolean, jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader() =
      new SapListBapisPartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options
      .get(SapDataSource.LIST_BAPIS_KEY)
      .flatMap(parse(_).extractOpt[Array[String]])
      .map(new SapListBapisReader(_, options))
}
