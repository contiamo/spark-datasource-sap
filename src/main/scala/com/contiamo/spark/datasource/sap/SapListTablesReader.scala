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

  protected val jcoTableReadFunName = "RFC_READ_TABLE"
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
      val dfOptions = compact(render((SapDataSource.TABLE_KEY -> table) ~ jcoOptions))

      val fs = collection.mutable.ArrayBuffer.empty[StructField]
      val tableReadFun = tableReadFunTemplate.getFunction
      val imports = tableReadFun.getImportParameterList
      val tables = tableReadFun.getTableParameterList

      imports.setValue("QUERY_TABLE", table)
      imports.setValue("NO_DATA", "Y")
      tableReadFun.execute(dest)

      val fieldsOut = tables.getTable("FIELDS")
      val fieldsOutMeta = fieldsOut.getRecordMetaData
      val nameIdx = fieldsOutMeta.indexOf("FIELDNAME")

      fieldsOut.firstRow()
      do {
        val fieldDesc = StructField(fieldsOut.getString(nameIdx), StringType)
        fs.append(fieldDesc)
      } while (fieldsOut.nextRow())
      val tableSchema = StructType(fs.toIndexedSeq).json

      currentRow.update(0, UTF8String.fromBytes(table.getBytes("UTF-8")))
      currentRow.update(1, UTF8String.fromBytes(tableSchema.getBytes("UTF-8")))
      currentRow.update(2, UTF8String.fromBytes(dfOptions.getBytes("UTF-8")))
    }

    hasNext
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}
