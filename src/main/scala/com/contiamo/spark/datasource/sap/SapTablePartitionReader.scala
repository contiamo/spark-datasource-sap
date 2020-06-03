package com.contiamo.spark.datasource.sap

import scala.util.chaining._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// https://www.se80.co.uk/sapfms/r/rfc_/rfc_read_table.htm
// filter pushdown https://stackoverflow.com/questions/27633332/rfc-read-table-passing-options-and-fields-parametrs-c
// todo check TABLE_ENTRIES_GET_VIA_RFC or RFC_GET_TABLE_ENTRIES or BBP_RFC_READ_TABLE

class SapTablePartitionReader(partition: SapDataSourceReader.TablePartition, schemaOnly: Boolean = false)
    extends SapSchemaReader
    with InputPartitionReader[InternalRow] {
  override def jcoOptions: Map[String, String] = partition.jcoOptions

  private val jcoTableReadFunName = "RFC_READ_TABLE"
  private val tableReadFun = Option(dest.getRepository.getFunction(jcoTableReadFunName)).get
  private val imports = Option(tableReadFun.getImportParameterList).get
  private val tables = Option(tableReadFun.getTableParameterList).get

  imports.setValue("QUERY_TABLE", partition.tableName)
  imports.setValue("DELIMITER", ";")

  if (schemaOnly) {
    imports.setValue("NO_DATA", "Y")
  }

  tableReadFun.execute(dest)

  private val fieldsOut = tables.getTable("FIELDS")
  fieldsOut.firstRow()

  private val fieldsOutMeta = fieldsOut.getRecordMetaData
  private val Seq(nameIdx, offsetIdx, lengthIdx, typeIdx) =
    Seq("FIELDNAME", "OFFSET", "LENGTH", "TYPE").map(fieldsOutMeta.indexOf)

  case class ReadTableField(idx: Int, name: String, offset: Int, length: Int, sapTypeName: String)
  private val fields = collection.mutable.ArrayBuffer.empty[ReadTableField]

  do {
    val fieldName = fieldsOut.getString(nameIdx)
    val fieldIdx = fields.length
    val fieldDesc = ReadTableField(fieldIdx,
                                   fieldName,
                                   fieldsOut.getInt(offsetIdx),
                                   fieldsOut.getInt(lengthIdx),
                                   fieldsOut.getString(typeIdx))
    fields.append(fieldDesc)
  } while (fieldsOut.nextRow())

  override val schema: StructType =
    fields
      .map(fieldDesc => StructField(fieldDesc.name, StringType))
      .pipe(StructType.apply)

  private val currentRow = new SpecificInternalRow(schema)
  private val data = tables.getTable("DATA")
  data.firstRow()

  override def next(): Boolean = data.nextRow()

  override def get(): InternalRow = {
    val rowStr = data.getString(0)
    for (fieldDesc <- fields) {
      val strValue =
        if (fieldDesc.offset + fieldDesc.length < rowStr.length)
          rowStr.substring(fieldDesc.offset, fieldDesc.offset + fieldDesc.length).trim
        else ""
      val sparkStrValue = UTF8String.fromBytes(strValue.getBytes("UTF-8"))
      currentRow.update(fieldDesc.idx, sparkStrValue)
    }

    currentRow
  }

  override def close(): Unit = {}
}
