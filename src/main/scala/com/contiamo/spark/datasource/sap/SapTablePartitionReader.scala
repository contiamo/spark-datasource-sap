package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapDataSourceReader.TablePartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

class SapTablePartitionReader(partition: TablePartition)
    extends SapTableSchemaReader(partition, noData = false)
    with InputPartitionReader[InternalRow] {

  private val currentRow = new SpecificInternalRow(schema)
  private val data = tables.getTable("DATA")
  private var firstRow = true

  override def next(): Boolean =
    if (firstRow) {
      data.firstRow()
      firstRow = false
      !data.isEmpty
    } else
      data.nextRow()

  override def get(): InternalRow = {
    val rowStr = data.getString(0)
    for (fieldDesc <- fields) {
      val fieldEnd = rowStr.length min (fieldDesc.offset + fieldDesc.length)
      val strValue = rowStr.substring(fieldDesc.offset, fieldEnd).trim
      val byteValue = data.getByteArray(0).slice(fieldDesc.offset, fieldEnd)
      val sparkValue = parseAtomicValue(strValue, byteValue, fieldDesc.length, fieldDesc.sparkType)
      currentRow.update(fieldDesc.idx, sparkValue)
    }

    currentRow
  }

  override def close(): Unit = {}
}
