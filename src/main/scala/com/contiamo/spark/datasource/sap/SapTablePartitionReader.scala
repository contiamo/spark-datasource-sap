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
  private val where = tables.getTable("OPTIONS")

  partition.tableFilters.flatMap(_.whereClauseLines).foreach { whereStr =>
    where.appendRow()
    where.setValue("TEXT", whereStr)
  }

  data.firstRow()

  override def next(): Boolean = data.nextRow()

  override def get(): InternalRow = {
    val rowStr = data.getString(0)
    for (fieldDesc <- fields) {
      val fieldEnd = rowStr.length min (fieldDesc.offset + fieldDesc.length)
      val strValue = rowStr.substring(fieldDesc.offset, fieldEnd).trim
      val sparkValue = parseAtomicValue(strValue, fieldDesc.sparkType)
      currentRow.update(fieldDesc.idx, sparkValue)
    }

    currentRow
  }

  override def close(): Unit = {}
}
