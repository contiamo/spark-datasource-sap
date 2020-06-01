package com.contiamo.spark.datasource.sap

import scala.util.chaining._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

// https://www.se80.co.uk/sapfms/r/rfc_/rfc_read_table.htm
// filter pushdown https://stackoverflow.com/questions/27633332/rfc-read-table-passing-options-and-fields-parametrs-c
// todo check TABLE_ENTRIES_GET_VIA_RFC or RFC_GET_TABLE_ENTRIES or BBP_RFC_READ_TABLE

class SapTablePartitionReader(partition: SapDataSourceReader.TablePartition)
    extends SapTableSchemaReader(partition)
    with InputPartitionReader[InternalRow] {

  override protected val schemaOnly: Boolean = false

  private val currentRow = new SpecificInternalRow(schema)
  private val data = tables.getTable("DATA")

  data.firstRow()

  override def next(): Boolean = data.nextRow()

  override def get(): InternalRow = {
    val rowStr = data.getString(0)
    for (fieldDesc <- fields) {
      val fieldEnd = rowStr.length min (fieldDesc.offset + fieldDesc.length)
      val strValue = rowStr.substring(fieldDesc.offset, fieldEnd).trim
      val sparkStrValue = UTF8String.fromBytes(strValue.getBytes("UTF-8"))
      currentRow.update(fieldDesc.idx, sparkStrValue)
    }

    currentRow
  }

  override def close(): Unit = {}
}
