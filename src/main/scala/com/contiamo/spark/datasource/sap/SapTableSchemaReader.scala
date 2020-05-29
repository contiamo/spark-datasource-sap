package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.types._

import scala.collection.immutable
import scala.util.chaining._

class SapTableSchemaReader(partition: SapDataSourceReader.TablePartition) extends SapSchemaReader {
  override def jcoOptions: Map[String, String] = partition.jcoOptions

  protected val schemaOnly: Boolean = true

  protected val jcoTableReadFunName = "RFC_READ_TABLE"
  protected val tableReadFun = Option(dest.getRepository.getFunction(jcoTableReadFunName)).get
  protected val imports = Option(tableReadFun.getImportParameterList).get
  protected val tables = Option(tableReadFun.getTableParameterList).get

  imports.setValue("QUERY_TABLE", partition.tableName)
  imports.setValue("DELIMITER", ";")

  if (schemaOnly) imports.setValue("NO_DATA", "Y")

  partition.requiredColumns.foreach { schema =>
    val fieldsIn = tables.getTable("FIELDS")
    schema.fieldNames.foreach { fieldName =>
      fieldsIn.appendRow()
      fieldsIn.setValue("FIELDNAME", fieldName)
    }
  }

  tableReadFun.execute(dest)

  case class ReadTableField(idx: Int, name: String, offset: Int, length: Int, sapTypeName: String)

  protected lazy val fields: immutable.IndexedSeq[ReadTableField] = {
    val fs = collection.mutable.ArrayBuffer.empty[ReadTableField]
    val fieldsOut = tables.getTable("FIELDS")
    val fieldsOutMeta = fieldsOut.getRecordMetaData
    val Seq(nameIdx, offsetIdx, lengthIdx, typeIdx) =
      Seq("FIELDNAME", "OFFSET", "LENGTH", "TYPE").map(fieldsOutMeta.indexOf)

    fieldsOut.firstRow()
    do {
      val fieldDesc = ReadTableField(fs.length,
                                     fieldsOut.getString(nameIdx),
                                     fieldsOut.getInt(offsetIdx),
                                     fieldsOut.getInt(lengthIdx),
                                     fieldsOut.getString(typeIdx))
      fs.append(fieldDesc)
    } while (fieldsOut.nextRow())
    fs.toIndexedSeq
  }

  override def schema: StructType =
    fields
      .map(fieldDesc => StructField(fieldDesc.name, StringType))
      .pipe(StructType.apply)
}
