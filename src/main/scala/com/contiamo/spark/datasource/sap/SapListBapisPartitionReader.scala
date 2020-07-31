package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapListBapisReader.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.chaining._

class SapListBapisPartitionReader(partition: Partition) extends SapSchemaReader with InputPartitionReader[InternalRow] {
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  override def jcoOptions: Map[String, String] = partition.jcoOptions

  override def schema: StructType =
    Seq("name", "defaultSchemaJson", "dynamicParameters", "dfOptions")
      .map(StructField(_, StringType))
      .pipe(StructType.apply)

  private val bapisIter = partition.bapis.toIterator
  private val currentRow = new SpecificInternalRow(schema)
  private val flattenFun =
    if (partition.bapiFlatten) (s: StructType) => flattenSparkSchema(s)._1
    else identity[StructType] _

  override def next(): Boolean = {
    val hasNext = bapisIter.hasNext

    if (hasNext) {
      val bapiName = bapisIter.next

      val funTemplate = Option(dest.getRepository.getFunctionTemplate(bapiName))
        .getOrElse(throw new RFCNotFoundException(bapiName))

      val defaultSchemaJson =
        Option(funTemplate.getExportParameterList)
          .map(sapMetaDataToSparkSchema)
          .map(flattenFun)
          .getOrElse(StructType.apply(Seq.empty))
          .json

      val dfOptions = compact(
        render(
          (SapDataSource.BAPI_KEY -> bapiName)
            ~ (SapDataSource.BAPI_FLATTEN_KEY -> partition.bapiFlatten)
            ~ jcoOptions))

      val dynamicParameters = compact(render(Seq(SapDataSource.BAPI_ARGS_KEY, SapDataSource.BAPI_OUTPUT_TABLE_KEY)))

      currentRow.update(0, UTF8String.fromString(bapiName))
      currentRow.update(1, UTF8String.fromString(defaultSchemaJson))
      currentRow.update(2, UTF8String.fromString(dynamicParameters))
      currentRow.update(3, UTF8String.fromString(dfOptions))
    }

    hasNext
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}
