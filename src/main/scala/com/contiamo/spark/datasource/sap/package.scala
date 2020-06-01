package com.contiamo.spark.datasource

import java.time.{LocalDateTime, ZoneOffset}

import scala.util.chaining._
import com.sap.conn.jco.{JCoMetaData, JCoRecord}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

package object sap {
  def sapToSparkType(typeCode: Int, bcdDecimals: Int = 38): DataType = typeCode match {
    case JCoMetaData.TYPE_CHAR    => StringType
    case JCoMetaData.TYPE_NUM     => StringType
    case JCoMetaData.TYPE_BYTE    => BinaryType
    case JCoMetaData.TYPE_BCD     => createDecimalType(bcdDecimals, 0)
    case JCoMetaData.TYPE_INT     => IntegerType
    case JCoMetaData.TYPE_INT1    => ByteType
    case JCoMetaData.TYPE_INT2    => ShortType
    case JCoMetaData.TYPE_FLOAT   => DoubleType
    case JCoMetaData.TYPE_DATE    => DateType
    case JCoMetaData.TYPE_TIME    => TimestampType
    case JCoMetaData.TYPE_DECF16  => createDecimalType(16, 0)
    case JCoMetaData.TYPE_DECF34  => createDecimalType(38, 0)
    case JCoMetaData.TYPE_STRING  => StringType
    case JCoMetaData.TYPE_XSTRING => BinaryType
    case _                        => StringType
  }

  def sapMetaDataToSparkSchema(meta: JCoMetaData): StructType =
    0.until(meta.getFieldCount)
      .map { i =>
        val sparkType =
          if (meta.isStructure(i))
            sapMetaDataToSparkSchema(meta.getRecordMetaData(i))
          else
            sapToSparkType(meta.getType(i), meta.getDecimals(i))
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
      LocalDateTime.ofInstant(rec.getDate(idx).toInstant, ZoneOffset.UTC).toLocalDate.toEpochDay.toInt
    case TimestampType =>
      rec.getDate(idx).getTime
    case BinaryType =>
      rec.getByteArray(idx)
    case StringType =>
      UTF8String.fromBytes(rec.getString(idx).getBytes("UTF-8"))
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
