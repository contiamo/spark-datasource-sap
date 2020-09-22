package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapListTablesReader.Partition
import org.apache.spark.sql.connector.read.Scan

class SapListTablesReader(tables: Seq[String], override val options: OptionsMap) extends SapDataSourceBaseReader {
  override def name(): String = SapDataSource.LIST_TABLES_KEY

  override def build(): Scan = new SapScan[Partition] {
    override val partition = Partition(tables, tableReadFun, jcoOptions)
    override def readSchema() = SapListTablesPartitionReader.schema
  }
}

object SapListTablesReader {
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  implicit val formats: DefaultFormats.type = DefaultFormats

  case class Partition(tables: Seq[String], jcoTableReadFunName: String, jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader() = new SapListTablesPartitionReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options
      .get(SapDataSource.LIST_TABLES_KEY)
      .flatMap(parse(_).extractOpt[Array[String]])
      .map(new SapListTablesReader(_, options))
}
