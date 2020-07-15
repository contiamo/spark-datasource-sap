package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapDataSourceReader.ListTablesPartition
import com.sap.conn.jco.{JCoFunction, JCoFunctionTemplate, JCoParameterList}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.chaining._

class SapListTablesReader(partition: ListTablesPartition)
    extends SapSchemaReader
    with InputPartitionReader[InternalRow] {
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  override def jcoOptions: Map[String, String] = partition.jcoOptions

  protected val jcoTableReadFunName = partition.jcoTableReadFunName
  protected val tableReadFunTemplate: JCoFunctionTemplate =
    Option(dest.getRepository.getFunctionTemplate(jcoTableReadFunName))
      .getOrElse(throw new RFCNotFoundException(jcoTableReadFunName))

  override val schema: StructType =
    Seq("name", "schemaJson", "dfOptions")
      .map(StructField(_, StringType))
      .pipe(StructType.apply)

  private val tablesIter = partition.tables.toIterator
  private val currentRow = new SpecificInternalRow(schema)

  override def next(): Boolean = {
    val hasNext = tablesIter.hasNext

    if (hasNext) {
      val table = tablesIter.next
      val dfOptions = compact(
        render(
          (SapDataSource.TABLE_KEY -> table)
            ~ (SapDataSource.TABLE_READ_FUN_KEY -> jcoTableReadFunName)
            ~ jcoOptions))

      val fs = collection.mutable.ArrayBuffer.empty[StructField]
      val tableReadFun = tableReadFunTemplate.getFunction
      val imports = tableReadFun.getImportParameterList
      val tables = tableReadFun.getTableParameterList

      imports.setValue("QUERY_TABLE", table)
      imports.setValue("NO_DATA", "Y")
      tableReadFun.execute(dest)

      val tableSchema = StructType(SapTableSchemaReader.parseFieldsMetadata(tables).map(_.structField)).json

      currentRow.update(0, UTF8String.fromString(table))
      currentRow.update(1, UTF8String.fromString(tableSchema))
      currentRow.update(2, UTF8String.fromString(dfOptions))
    }

    hasNext
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}
