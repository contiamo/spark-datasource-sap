package com.contiamo.spark.datasource.sap

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

//with SupportsPushDownRequiredColumns
//with SupportsPushDownFilters

class SapDataSourceReader(mockdata: String) extends DataSourceReader {
  import scala.util.chaining._

  private val data = mockdata.split(";").map(_.split(","))

  override def readSchema(): StructType =
    data.headOption
      .getOrElse(Array.empty)
      .zipWithIndex
      .map(_._2)
      .map(idx => StructField(idx.toString, StringType))
      .pipe(StructType.apply)

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    factoryList.add(new SapDataSourceReader.Partition(data))
    factoryList
  }
}

object SapDataSourceReader {
  class Partition(data: Array[Array[String]]) extends InputPartition[InternalRow] {
    override def createPartitionReader(): InputPartitionReader[InternalRow] = new SapDataSourcePartitionReader(data)
  }
}
