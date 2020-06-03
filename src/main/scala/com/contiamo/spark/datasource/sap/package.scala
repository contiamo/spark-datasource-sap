package com.contiamo.spark.datasource

import scala.util.chaining._
import com.sap.conn.jco.JCoMetaData
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataType, StructField, StructType}

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
        val sparkType = sapToSparkType(meta.getType(i), meta.getDecimals(i))
        StructField(meta.getName(i), sparkType)
      }
      .pipe(StructType.apply)

}
