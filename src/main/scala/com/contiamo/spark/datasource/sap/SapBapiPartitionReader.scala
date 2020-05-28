package com.contiamo.spark.datasource.sap

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import com.sap.conn.jco.{JCoMetaData, JCoRecord}

import scala.util.chaining._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonAST.{JArray, JBool, JDecimal, JDouble, JInt, JLong, JString}

import scala.collection.AbstractIterator

class SapBapiPartitionReader(partition: SapDataSourceReader.BapiPartition, schemaOnly: Boolean = false)
    extends SapSchemaReader
    with InputPartitionReader[InternalRow] {
  override def jcoOptions: Map[String, String] = partition.jcoOptions

  case class OutputSrc(data: Iterator[JCoRecord], schema: JCoMetaData)

  private def extractExportOutput: Option[OutputSrc] =
    Option(fun.getExportParameterList)
      .flatMap { exportFields =>
        val hasField = exportFields.getListMetaData.hasField(partition.bapiOutput)
        val isStruct = exportFields.getListMetaData.isStructure(partition.bapiOutput)
        val isTable = exportFields.getListMetaData.isTable(partition.bapiOutput)

        if (hasField && isStruct) Option(exportFields.getStructure(partition.bapiOutput))
        else if (hasField && isTable) Option(exportFields.getTable(partition.bapiOutput))
        else if (hasField) Option(exportFields)
        else Option(exportFields)
      }
      .map { src =>
        OutputSrc(Iterator.apply(src), src.getMetaData)
      }

  private def extractTableOutput: Option[OutputSrc] =
    Option(fun.getTableParameterList).flatMap { tables =>
      if (tables.getListMetaData.hasField(partition.bapiOutput)) {
        val table = tables.getTable(partition.bapiOutput)

        val tableIter = new AbstractIterator[JCoRecord] {
          private var rowId = 0
          override def hasNext: Boolean = rowId < table.getNumRows
          override def next(): JCoRecord = {
            table.setRow(rowId)
            rowId += 1
            table
          }
        }

        Option(OutputSrc(tableIter, table.getRecordMetaData))
      } else None
    }

  private val fun = Option(dest.getRepository.getFunction(partition.funName.toUpperCase)).get
  private val output = extractTableOutput.orElse(extractExportOutput)

  override val schema: StructType =
    output
      .map(_.schema)
      .map(sapMetaDataToSparkSchema)
      .getOrElse(StructType(Seq.empty))

  // parse & bind input parameters, then execute the call
  if (!schemaOnly) {
    val imports =
      Option(fun.getImportParameterList).get

    partition.bapiArgs.foreach {
      case (param, jsonValue) =>
        val paramName = param.toUpperCase
        jsonValue match {
          case JString(v)   => imports.setValue(paramName, v)
          case JInt(v)      => imports.setValue(paramName, v)
          case JLong(v)     => imports.setValue(paramName, v)
          case JBool(true)  => imports.setValue(paramName, 'X')
          case JBool(false) => imports.setValue(paramName, ' ')
          case JDouble(v)   => imports.setValue(paramName, v)
        }
    }

    fun.execute(dest)
  }

  private val currentRow = new SpecificInternalRow(schema)
  private val data: Iterator[JCoRecord] = output.map(_.data).getOrElse(Iterator.empty)

  override def next(): Boolean = {
    val hasNext = data.hasNext

    if (hasNext) {
      val rec = data.next
      for ((field, idx) <- schema.fields.zipWithIndex) {
        if (rec.getValue(idx) != null) {
          val anyval = field.dataType match {
            case IntegerType =>
              rec.getInt(idx)
            case DoubleType =>
              rec.getDouble(idx)
            case DateType =>
              LocalDateTime.ofInstant(rec.getDate(idx).toInstant, ZoneOffset.UTC).toLocalDate.toEpochDay.toInt
            case TimestampType =>
              rec.getDate(idx).getTime
            case BinaryType =>
              rec.getByteArray(idx)
            case StringType =>
              UTF8String.fromBytes(rec.getString(idx).getBytes("UTF-8"))
          }
          currentRow.update(idx, anyval)
        } else {
          currentRow.setNullAt(idx)
        }
      }
    }

    hasNext
  }
  override def get(): InternalRow = currentRow
  override def close(): Unit = {}
}
