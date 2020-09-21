import java.sql.Date
import java.util.{Date => JDate}

import WhereClauseGen.ColumnTemplate
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen._
import org.scalacheck._

object WhereClauseGen {
  case class ColumnTemplate(name: String, typ: DataType, realValues: Seq[Any] = Seq.empty) {
    val column: Column = col(name)

    val genValue: Gen[Any] =
      if (realValues.isEmpty) WhereClauseGen.genValue(typ)
      else
        frequency(
          (5, oneOf(realValues)),
          (1, WhereClauseGen.genValue(typ))
        )

    def genLikeValue: Gen[String] =
      for {
        wildcard <- oneOf(
          (s: String) => "%" + s,
          (s: String) => s + "%",
          (s: String) => "%" + s + "%",
          (s: String) => s
        )
        v <- genValue
      } yield wildcard(v.toString.replace("\\", "\\\\"))

    def genOp: Gen[Column] = {
      val x = column
      val commonOps = oneOf(
        Gen.const(x.isNull),
        Gen.const(x.isNotNull),
        /*
         Since we treat [[Gen]] as a monad
         this is a bit counterintuitive to read.
         It will produce:
           x.equalTo(generatedValue)
         etc.
         */
        genValue.map(x.equalTo),
        genValue.map(x.notEqual),
        // Spark throws an NPE when you do IN (1, 2, NULL)
        Gen.listOf(genValue.suchThat(_ != null)).map(x.isInCollection)
      )
      val orderedOps = oneOf(
        genValue.map(x.leq),
        genValue.map(x.geq),
        genValue.map(x.lt),
        genValue.map(x.gt)
      )
      val strOps = genLikeValue.map(x.like)

      if (typ == StringType) oneOf(commonOps, strOps)
      else oneOf(commonOps, orderedOps)
    }
  }

  def genValue(colType: DataType): Gen[Any] = colType match {
    /*
     According to the spec, we represent even the
     smallest of integral columns as Int, so, when
     we generate, we have to pick the smallest
     */
    case IntegerType => posNum[Byte]
    case DoubleType  => arbitrary[Double]
    case StringType  => asciiPrintableStr
    case DateType =>
      arbitrary[JDate]
        .map(_.getTime)
        .filter(t => Math.floorDiv(t, DateTimeConstants.MILLIS_PER_DAY) < Int.MaxValue)
        .map(t => new Date(t))
    case _: DecimalType => arbitrary[BigDecimal]
    case _              => Gen.const(null)
  }

  def apply(allColumns: Seq[ColumnTemplate]): Gen[Column] =
    new WhereClauseGen(allColumns).genExpr
}

class WhereClauseGen(allColumns: Seq[ColumnTemplate]) {
  def genColumn: Gen[Column] =
    oneOf(allColumns).flatMap(_.genOp)

  def genAnd: Gen[Column] =
    for {
      a <- genExpr
      b <- genExpr
    } yield a.and(b)

  def genOr: Gen[Column] =
    for {
      a <- genExpr
      b <- genExpr
    } yield a.or(b)

  def genNot: Gen[Column] = genExpr.map(e => e.unary_!)

  def genExpr: Gen[Column] = oneOf(genColumn, lzy(oneOf(genAnd, genOr, genNot)))
}
