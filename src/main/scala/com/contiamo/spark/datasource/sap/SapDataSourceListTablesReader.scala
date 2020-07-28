package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapDataSourceListTablesReader.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

class SapDataSourceListTablesReader(tables: Seq[String], override val options: OptionsMap)
    extends SapDataSourceBaseReader {
  private val partition = Partition(tables, tableReadFun, jcoOptions)
  override val partitions = Seq(partition)
  override def schemaReader = new SapListTablesReader(partition)
}

object SapDataSourceListTablesReader {
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  implicit val formats: DefaultFormats.type = DefaultFormats

  case class Partition(tables: Seq[String], jcoTableReadFunName: String, jcoOptions: Map[String, String])
      extends SapInputPartition {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapListTablesReader(this)
  }

  def apply(options: OptionsMap): Option[SapDataSourceBaseReader] =
    options
      .get(SapDataSource.LIST_TABLES_KEY)
      .flatMap(parse(_).extractOpt[Array[String]])
      .map(new SapDataSourceListTablesReader(_, options))
}
