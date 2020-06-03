package com.contiamo.spark.datasource.sap

import java.util

import com.sap.conn.jco.JCoDestinationManager

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

//with SupportsPushDownRequiredColumns
//with SupportsPushDownFilters

class SapDataSourceReader(options: DataSourceOptions) extends DataSourceReader {
  import scala.util.chaining._

  private val sapOptions =
    options
      .asMap()
      .asScala
      .filterKeys(_ != "mockdata")
      .map {
        case (k, v) => (s"jco.client.$k", v)
      }
      .toMap

  private val destKey = SapSparkDestinationDataProvider.register(sapOptions)
  private val dest = JCoDestinationManager.getDestination(destKey)
  dest.ping()
  private val pingFun = dest.getRepository.getFunction("RFC_PING")
  pingFun.execute(dest)

  private val mockdata = options.get("mockdata").get()
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
    override def createPartitionReader(): InputPartitionReader[InternalRow] =
      new SapDataSourcePartitionReader(data)
  }
}
