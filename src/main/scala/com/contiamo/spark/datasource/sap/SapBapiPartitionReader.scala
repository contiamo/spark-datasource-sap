package com.contiamo.spark.datasource.sap

import com.sap.conn.jco.{JCoMetaData, JCoParameterList, JCoRecord}

import scala.util.chaining._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JBool, JDecimal, JDouble, JInt, JLong, JString}

import scala.collection.{AbstractIterator, mutable}

class SapBapiPartitionReader(partition: SapDataSourceReader.BapiPartition, schemaOnly: Boolean = false)
    extends SapSchemaReader
    with InputPartitionReader[InternalRow] {
  override def jcoOptions: Map[String, String] = partition.jcoOptions

  case class OutputSrc(data: Iterator[JCoRecord], schema: JCoMetaData)

  private def extractExportOutput: Option[OutputSrc] =
    Option(fun.getExportParameterList)
      .map { exports =>
        applyPartialProjectionPushdown(exports)
        OutputSrc(Iterator.apply(exports), exports.getMetaData)
      }

  /* Partial pushdown for export parameters.

     Setting an export parameter as inactive
     prevents it from being fetched from a server.
     Additionally `readRecord` calls `isInitialized`
     on every field checking whether it needs to be
     deserialized.
   */
  private def applyPartialProjectionPushdown(exports: JCoParameterList): Unit =
    partition.requiredColumns.foreach { cols =>
      val requitedFieldNames = cols.fieldNames
      val meta = exports.getListMetaData
      0.until(meta.getFieldCount).foreach { fidx =>
        val exportFieldName = meta.getName(fidx)
        val required =
          if (partition.bapiFlatten) requitedFieldNames.exists(_.startsWith(exportFieldName))
          else requitedFieldNames.contains(exportFieldName)

        exports.setActive(fidx, required)
      }
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

  private val fun = Option(dest.getRepository.getFunction(partition.funName)).get
  private val output = extractTableOutput.orElse(extractExportOutput).get

  private val pureSchema = sapMetaDataToSparkSchema(output.schema)
  private val (readySchema, recordRemapping) =
    if (partition.bapiFlatten)
      flattenSparkSchema(pureSchema)
    else (pureSchema, Map.empty: SchemaRemapping)

  override val schema: StructType = readySchema

  // parse & bind input parameters, then execute the call
  if (!schemaOnly) {
    val imports =
      Option(fun.getImportParameterList).get

    partition.bapiArgs.foreach {
      case (param, jsonValue) =>
        val paramName = param.toUpperCase
        jsonValue match {
          // TODO arrays and objects
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
  private val data: Iterator[JCoRecord] = output.data

  override def next(): Boolean = {
    val hasNext = data.hasNext

    if (hasNext) {
      val rec = data.next

      if (partition.bapiFlatten) readRecordFlat(rec, recordRemapping, currentRow)
      else readRecord(rec, schema, currentRow)
    }

    hasNext
  }

  override def get(): InternalRow = currentRow
  override def close(): Unit = {}
}
