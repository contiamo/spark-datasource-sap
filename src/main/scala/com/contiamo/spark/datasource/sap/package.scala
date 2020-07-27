package com.contiamo.spark.datasource

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}

import scala.util.chaining._
import com.sap.conn.jco.{JCoMetaData, JCoRecord}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.time.LocalTime
import java.util.TimeZone

import org.apache.spark.sql.catalyst.util.DateTimeUtils

package object sap {
  class SapDataSourceException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
    def this(msg: String) = this(msg, null)
  }

  class SapEntityNotFoundException(entityType: String, name: String)
      extends SapDataSourceException(s"$entityType $name not found.")

  class SapReadTableBufferExceededException(table: String, cause: com.sap.conn.jco.AbapException)
      extends SapDataSourceException(
        s"""Selected columns of table '$table' do not fit into a configured read buffer.
            |Please, either select a smaller subset of columns, or configure a custom read table function.
            |""".stripMargin,
        cause
      )

  class RFCNotFoundException(name: String) extends SapEntityNotFoundException("SAP RFC", name)

  class InvalidConfigurationException(message: String) extends SapDataSourceException(message)

  class NoParamaterList(listKind: String, entity: String)
      extends SapDataSourceException(s"Unable to retrieve $listKind parameter list for $entity.")

  class ErrorParsingValueFromRfcReadTable(strValue: String, expectedType: DataType)
      extends SapDataSourceException(s"Unable to parse a value of type $expectedType from string: '$strValue'")

  def sapCodeToSparkType(typeCode: Int, bcdDecimalPrecision: Int): DataType = typeCode match {
    case JCoMetaData.TYPE_CHAR    => StringType
    case JCoMetaData.TYPE_NUM     => StringType
    case JCoMetaData.TYPE_BYTE    => BinaryType
    case JCoMetaData.TYPE_INT     => IntegerType
    case JCoMetaData.TYPE_INT1    => ByteType
    case JCoMetaData.TYPE_INT2    => ShortType
    case JCoMetaData.TYPE_FLOAT   => DoubleType
    case JCoMetaData.TYPE_DATE    => DateType
    case JCoMetaData.TYPE_TIME    => TimestampType
    case JCoMetaData.TYPE_DECF16  => DecimalType.SYSTEM_DEFAULT
    case JCoMetaData.TYPE_DECF34  => DecimalType.SYSTEM_DEFAULT
    case JCoMetaData.TYPE_STRING  => StringType
    case JCoMetaData.TYPE_XSTRING => StringType
    case JCoMetaData.TYPE_BCD =>
      createDecimalType(bcdDecimalPrecision, bcdDecimalPrecision min DecimalType.MAX_SCALE)
    case _ => StringType
  }

  def sapLetterToSparkType(typeCode: String): DataType = typeCode.toLowerCase match {
    case "c" => StringType
    case "n" => StringType
    case "x" => StringType
    case "p" => DecimalType.SYSTEM_DEFAULT // BCD
    case "i" => IntegerType
    case "b" => IntegerType
    case "s" => IntegerType
    case "f" => DoubleType
    case "d" => DateType
    case "t" => TimestampType
    case _   => StringType
  }

  protected[sap] val sapDateStrFmt = new SimpleDateFormat("yyyyMMdd")
  protected[sap] val sapTimeStrFmt = new SimpleDateFormat("yyyy-MM-dd HHmmss")

  def parseAtomicValue(extractedStrValue: String, maxLen: Int, dataType: DataType): Any =
    try {
      dataType match {
        case _ if extractedStrValue.forall(_ == '0') && (extractedStrValue.length == maxLen) => null
        case StringType =>
          UTF8String.fromString(extractedStrValue)
        case _ if extractedStrValue.isEmpty => null
        case IntegerType =>
          extractedStrValue.toInt
        // untested
        case DoubleType =>
          extractedStrValue.toDouble
        //20200629 -> 29-06-2020
        case DateType =>
          // both `millisToDays` and `parse` use the default timezone,
          // so it should match and produce the correct integer seconds value
          DateTimeUtils.millisToDays(sapDateStrFmt.parse(extractedStrValue).getTime)
        // 102050 -> 10:20:50
        case TimestampType =>
          val locT = sapTimeStrFmt.parse("1970-01-01 " + extractedStrValue).getTime
          DateTimeUtils.fromMillis(locT)
        case _decimal: DecimalType if !extractedStrValue.startsWith("*") =>
          Decimal.fromDecimal(BigDecimal.exact(extractedStrValue))
      }
    } catch {
      case err: Throwable =>
        val newErr = new ErrorParsingValueFromRfcReadTable(extractedStrValue, dataType)
        newErr.addSuppressed(err)
        throw newErr
    }

  def sapMetaDataToSparkSchema(meta: JCoMetaData): StructType =
    0.until(meta.getFieldCount)
      .map { i =>
        val sparkType =
          if (meta.isStructure(i))
            sapMetaDataToSparkSchema(meta.getRecordMetaData(i))
          else
            sapCodeToSparkType(meta.getType(i), meta.getDecimals(i))
        StructField(meta.getName(i), sparkType)
      }
      .pipe(StructType.apply)

  case class SchemaRemappingField(sparkType: DataType, path: Seq[Int])
  type SchemaRemapping = Map[Int, SchemaRemappingField]

  def flattenSparkSchema(schema: StructType): (StructType, SchemaRemapping) = {
    @scala.annotation.tailrec
    def flattenSparkSchemaTail(pendingFields: Seq[StructField],
                               pendingPaths: Vector[Vector[Int]],
                               assembledPaths: Vector[SchemaRemappingField] = Vector.empty,
                               assembledSchema: Vector[StructField] = Vector.empty): (StructType, SchemaRemapping) =
      if (pendingFields.isEmpty) {
        (StructType(assembledSchema), assembledPaths.zipWithIndex.map(_.swap).toMap)
      } else {
        val field = pendingFields.head
        val tail = pendingFields.tail

        val pathsHead = pendingPaths.head
        val pathsTail = pendingPaths.tail

        field.dataType match {
          case st: StructType =>
            val prefixed = st.fields.map(f => f.copy(name = field.name + "_" + f.name))
            val prefixedPaths = st.fields.zipWithIndex.map(x => pathsHead :+ x._2).toVector
            flattenSparkSchemaTail(prefixed ++ tail, prefixedPaths ++ pathsTail, assembledPaths, assembledSchema)
          case _ =>
            val assembledPath = SchemaRemappingField(field.dataType, pathsHead)
            flattenSparkSchemaTail(tail, pathsTail, assembledPaths :+ assembledPath, assembledSchema :+ field)
        }
      }

    flattenSparkSchemaTail(schema.fields, schema.fields.zipWithIndex.map(i => Vector(i._2)).toVector)
  }

  private def readNonNullAtomicType(rec: JCoRecord, idx: Int): PartialFunction[DataType, Any] = {
    case IntegerType =>
      rec.getInt(idx)
    case DoubleType =>
      rec.getDouble(idx)
    case DateType =>
      DateTimeUtils.millisToDays(rec.getDate(idx).getTime)
    case TimestampType =>
      DateTimeUtils.fromMillis(rec.getTime(idx).getTime)
    case BinaryType =>
      rec.getByteArray(idx)
    case StringType =>
      UTF8String.fromString(rec.getString(idx))
    case _decimal: DecimalType =>
      Decimal.fromDecimal(rec.getBigDecimal(idx))
  }

  private def readAtomicType(rec: JCoRecord, idx: Int): PartialFunction[DataType, Any] =
    if (rec.isInitialized(idx) && rec.getValue(idx) != null) readNonNullAtomicType(rec, idx)
    else { case _ => null }

  def readRecord(rec: JCoRecord, schema: StructType, outRec: SpecificInternalRow): Unit =
    for ((field, idx) <- schema.fields.zipWithIndex) {
      val recursiveReader: PartialFunction[DataType, Any] = readAtomicType(rec, idx) orElse {
        case struct: StructType =>
          var outRec1 = outRec.get(idx, struct).asInstanceOf[SpecificInternalRow]
          if (outRec1 == null)
            outRec1 = new SpecificInternalRow(struct)

          readRecord(rec.getStructure(idx), struct, outRec1)
          outRec1
      }
      outRec.update(idx, recursiveReader(field.dataType))
    }

  def readRecordFlat(rec0: JCoRecord, remapping: SchemaRemapping, outRec: SpecificInternalRow): Unit =
    for ((fidx, remappingField) <- remapping) {
      val (rec, idx) = remappingField.path match {
        case Seq()  => (rec0, fidx)
        case Seq(i) => (rec0, i)
        case path =>
          val p = path.reverse
          val rec1 = p.tail.foldLeft(rec0) { case (recA, pi) => recA.getStructure(pi) }
          (rec1, p.head)
      }

      val value = readAtomicType(rec, idx)(remappingField.sparkType)
      outRec.update(fidx, value)
    }
}
