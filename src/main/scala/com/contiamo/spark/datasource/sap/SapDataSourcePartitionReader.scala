package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

import scala.collection.JavaConverters._

class SapDataSourcePartitionReader(data: Array[Array[String]])
    extends InputPartitionReader[InternalRow] {
  import scala.util.chaining._

  private val iterator = data.toIterator.asJava
  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = {
    iterator
      .next()
      .map { value =>
        org.apache.spark.unsafe.types.UTF8String
          .fromBytes(value.getBytes("UTF-8"))
      }
      .toSeq
      .pipe(InternalRow.fromSeq)
  }

  override def close(): Unit = {}
}
