package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapBapiReader.Partition
import com.sap.conn.jco.{JCoMetaData, JCoParameterList, JCoRecord}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.json4s.JsonAST._

import scala.collection.AbstractIterator

class SapBapiPartitionReader(partition: Partition, schemaOnly: Boolean = false)
    extends SapSchemaReader
    with PartitionReader[InternalRow] {
  override def jcoOptions: Map[String, String] = partition.jcoOptions

  case class OutputSrc(data: Iterator[JCoRecord], schema: JCoMetaData)

  private def extractExportOutput: OutputSrc =
    Option(fun.getExportParameterList)
      .map { exports =>
        applyPartialProjectionPushdown(exports)
        OutputSrc(Iterator.apply(exports), exports.getMetaData)
      }
      .getOrElse(throw new NoParamaterList("exports", partition.funName))

  /* Partial pushdown for export parameters.

     Setting an export parameter as inactive
     prevents it from being fetched from a server.
     Additionally `readRecord` calls `isInitialized`
     on every field checking whether it needs to be
     deserialized.
   */
  private def applyPartialProjectionPushdown(exports: JCoParameterList): Unit =
    partition.requiredColumns.foreach { cols =>
      val requiredFieldNames = cols.fieldNames
      val meta = exports.getListMetaData
      0.until(meta.getFieldCount).foreach { fidx =>
        val exportFieldName = meta.getName(fidx)
        val required =
          if (partition.bapiFlatten) requiredFieldNames.exists(_.startsWith(exportFieldName))
          else requiredFieldNames.contains(exportFieldName)

        exports.setActive(fidx, required)
      }
    }

  private def extractTableOutput(outputTable: String): Option[OutputSrc] =
    Option(fun.getTableParameterList).flatMap { tables =>
      if (tables.getListMetaData.hasField(outputTable)) {
        val table = tables.getTable(outputTable)

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

  private val fun = Option(dest.getRepository.getFunction(partition.funName))
    .getOrElse(throw new RFCNotFoundException(partition.funName))

  private val output =
    if (partition.bapiOutputTable.isDefined)
      partition.bapiOutputTable
        .flatMap(extractTableOutput)
        .getOrElse(throw new InvalidConfigurationException(
          s"Unable to bind output table ${partition.bapiOutputTable} for ${partition.funName}"))
    else extractExportOutput

  private val pureSchema = sapMetaDataToSparkSchema(output.schema)
  private val (readySchema, recordRemapping) =
    if (partition.bapiFlatten)
      flattenSparkSchema(pureSchema)
    else (pureSchema, Map.empty: SchemaRemapping)

  override val schema: StructType = readySchema

  private def setAtomicParameterFromJson(rec: JCoRecord, paramName: String, jsonValue: JValue): Unit = {
    jsonValue match {
      case JString(v)   => rec.setValue(paramName, v)
      case JInt(v)      => rec.setValue(paramName, v.bigInteger.longValue())
      case JLong(v)     => rec.setValue(paramName, v)
      case JBool(true)  => rec.setValue(paramName, 'X')
      case JBool(false) => rec.setValue(paramName, ' ')
      case JDouble(v)   => rec.setValue(paramName, v)
      case v            => throw new InvalidConfigurationException(s"Value $v is not supported for the paramater $paramName")
    }
  }

  // parse & bind input parameters, then execute the call
  if (!schemaOnly) {
    val imports =
      Option(fun.getImportParameterList).getOrElse(throw new NoParamaterList("imports", partition.funName))
    val tables = Option(fun.getTableParameterList)

    partition.bapiArgs.foreach {
      case (param, jsonValue) =>
        val paramName = param.toUpperCase
        jsonValue match {
          case JArray(values) =>
            val tab = tables
              .flatMap { tables =>
                if (tables.getListMetaData.hasField(paramName)) Option(tables.getTable(paramName))
                else None
              }
              .getOrElse(throw new InvalidConfigurationException(
                s"parameter $paramName is passed as a table, but ${partition.funName} RFC doesn't have it"))

            values.foreach {
              case JObject(fields) =>
                tab.appendRow()
                fields.foreach {
                  case (subParamName, subParamValue) =>
                    setAtomicParameterFromJson(tab, subParamName.toUpperCase, subParamValue)
                }
              case _ =>
                throw new InvalidConfigurationException(
                  s"Table parameter $paramName of ${partition.funName} must be represented as by an array of objects")
            }
          case _ =>
            setAtomicParameterFromJson(imports, paramName, jsonValue)
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
