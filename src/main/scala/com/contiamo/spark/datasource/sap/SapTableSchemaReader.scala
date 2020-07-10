package com.contiamo.spark.datasource.sap

import com.contiamo.spark.datasource.sap.SapDataSourceReader.TablePartition
import com.sap.conn.jco.{JCoFunction, JCoParameterList}
import org.apache.spark.sql.types._

import scala.collection.immutable
import scala.util.chaining._

class SapTableSchemaReader(partition: TablePartition, noData: Boolean) extends SapSchemaReader {
  override def jcoOptions: Map[String, String] = partition.jcoOptions

  protected val jcoTableReadFunName = partition.jcoTableReadFunName
  protected val tableReadFun: JCoFunction = Option(dest.getRepository.getFunction(jcoTableReadFunName))
    .getOrElse(throw new RFCNotFoundException(jcoTableReadFunName))
  protected val imports: JCoParameterList = Option(tableReadFun.getImportParameterList)
    .getOrElse(throw new NoParamaterList("imports", jcoTableReadFunName))
  protected val tables: JCoParameterList = Option(tableReadFun.getTableParameterList)
    .getOrElse(throw new NoParamaterList("tables", jcoTableReadFunName))

  imports.setValue("QUERY_TABLE", partition.tableName)
  imports.setValue("DELIMITER", ";")

  if (noData) imports.setValue("NO_DATA", "Y")

  partition.requiredColumns.foreach { schema =>
    val fieldsIn = tables.getTable("FIELDS")
    schema.fieldNames.foreach { fieldName =>
      fieldsIn.appendRow()
      fieldsIn.setValue("FIELDNAME", fieldName)
    }
  }

  if (!noData) {
    val where = tables.getTable("OPTIONS")

    partition.whereClauseLines.foreach { whereStr =>
      where.appendRow()
      where.setValue("TEXT", whereStr)
    }
  }

  tableReadFun.execute(dest)

  case class ReadTableField(idx: Int, name: String, offset: Int, length: Int, sapTypeName: String) {
    val sparkType: DataType = sapLetterToSparkType(sapTypeName)
    def structField: StructField = StructField(name, sparkType)
  }

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

  override def schema = StructType(fields.map(_.structField))
}
