import java.sql.Date
import java.util.{Date => JDate}

import org.scalacheck._
import Gen._
import Arbitrary.arbitrary
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.functions._

object WhereClauseGen {
  def genValue(colType: DataType): Gen[Any] = colType match {
    case StringType     => arbitrary[String]
    case IntegerType    => arbitrary[Int]
    case DoubleType     => arbitrary[Double]
    case DateType       => arbitrary[JDate].map(d => new Date(d.getTime))
    case _: DecimalType => arbitrary[BigDecimal]
    case _              => Gen.const(null)
  }

  def genOp(x: Column, colType: DataType): Gen[Column] = oneOf(
    Gen.const(x.isNull),
    Gen.const(x.isNotNull),
    // Since we treat [[Gen]] as a monad
    // this is a bit counterintuitive to read.
    // It will produce:
    //   x.equalTo(generatedValue)
    // etc.
    genValue(colType).map(x.equalTo),
    genValue(colType).map(x.notEqual),
    genValue(colType).map(x.leq),
    genValue(colType).map(x.geq),
    genValue(colType).map(x.lt),
    genValue(colType).map(x.gt),
    Gen.listOf(genValue(colType)).map(x.isInCollection)
  )

  def generateWhereClause(allColumns: Seq[(String, DataType)]): Gen[Column] = {
    def genColumn =
      oneOf(allColumns).flatMap {
        case (colName, colType) =>
          genOp(col(colName), colType)
      }

    def genAnd =
      for {
        a <- genExpr
        b <- genExpr
      } yield a.and(b)

    def genOr =
      for {
        a <- genExpr
        b <- genExpr
      } yield a.or(b)

    def genNot = genExpr.map(e => e.unary_!)

    def genExpr: Gen[Column] = oneOf(genColumn, lzy(oneOf(genAnd, genOr, genNot)))

    genExpr
  }
}
