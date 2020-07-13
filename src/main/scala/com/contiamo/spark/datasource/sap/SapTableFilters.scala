package com.contiamo.spark.datasource.sap

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

class SapTableFilters(filters: Array[Filter], schema: StructType) {
  import SapTableFilters._

  private val (pushed_, rejected_) = filters
    .map(SapTableFilter(_, schema))
    .partition(_.whereClauseLines.isRight)

  val pushed: Array[Filter] = pushed_.map(_.filter)
  val rejected: Array[Filter] = rejected_.map(_.filter)

  val whereClauseParts: Seq[String] = pushed_.zipWithIndex.flatMap {
    case (f, 0) => f.whereClauseLines.right.get
    case (f, _) => " AND " +: f.whereClauseLines.right.get
  }

  val whereClauseLines: Seq[String] = {
    val lines = mutable.ArrayBuffer.empty[String]
    val remainder = whereClauseParts.foldLeft("") {
      case (whereCombined, wherePart) =>
        val newCombined = whereCombined + wherePart
        if (newCombined.length < sapFilterLenLimit) {
          newCombined
        } else {
          lines += whereCombined
          wherePart
        }
    }

    if (remainder.nonEmpty)
      lines += remainder

    lines.toSeq
  }

  /* potentially useful for debugging in the future
  whereClauseLines.reduceOption(_ ++ _).foreach(pd => println(s"Pushed: " + pd))

  rejected_.foreach { f =>
    println(s"Not pushed down: ${f.whereClauseLines.left.get}")
  }
  */
}

object SapTableFilters {
  val sapFilterLenLimit = 72

  type PushdownResult = Either[String, Seq[String]]

  case class SapTableFilter(filter: Filter, schema: StructType) {
    @tailrec
    protected final def formatValue(attr: String, x: Any): Either[String, String] = x match {
      case d: Timestamp =>
        formatValue(attr, sapTimeStrFmt.format(d).dropWhile(_ != ' ').trim)
      case d: Date =>
        formatValue(attr, sapDateStrFmt.format(d))
      case _ =>
        // SAP fails on literal values that are larger than corresponding table attributes
        val fieldLen = Try(schema.apply(attr).metadata.getLong("length")).toOption
        if (fieldLen.isDefined && x.toString.length > fieldLen.get)
          Left(s"'$x' is wider than ${fieldLen.get}")
        else
          Try(org.apache.spark.sql.catalyst.expressions.Literal(x))
            .map(_.sql)
            .toEither
            .left
            .map(_.getMessage)
    }

    protected def formatBinOp(op: String, attr: String, value: Any): PushdownResult =
      formatValue(attr, value).map(v => Seq.empty :+ "(" :+ attr :+ s" $op " :+ v :+ ")")

    protected def formatBoolOp(op: String, left: Filter, right: Filter): PushdownResult =
      for {
        wl <- generateWhere(left)
        wr <- generateWhere(right)
      } yield ("(" +: wl) ++ Seq(s" $op ") ++ (wr :+ ")")

    protected def generateWhere(filter: Filter): PushdownResult = filter match {
      /*
       It's impossible to reliably tell a NULL value
       from a bunch of zeroes in RFC_READ_TABLE output.
       Therefore its impossible to implement
       a robust push-down for IS (NOT) NULL

       So, the intentionally left out filters are:
       - IsNull
       - IsNotNull
       - EqualNullSafe
       */

      case EqualTo(attr, value) =>
        formatBinOp("EQ", attr, value)
      case Not(EqualTo(attr, value)) =>
        formatBinOp("NE", attr, value)
      case LessThan(attr, value) =>
        formatBinOp("LT", attr, value)
      case LessThanOrEqual(attr, value) =>
        formatBinOp("LE", attr, value)
      case GreaterThan(attr, value) =>
        formatBinOp("GT", attr, value)
      case GreaterThanOrEqual(attr, value) =>
        formatBinOp("GE", attr, value)
      case StringContains(attr, value) =>
        formatBinOp("LIKE", attr, "%" + value + "%")
      case StringStartsWith(attr, value) =>
        formatBinOp("LIKE", attr, value + "%")
      case StringEndsWith(attr, value) =>
        formatBinOp("LIKE", attr, "%" + value)
      case And(left, right) =>
        formatBoolOp("AND", left, right)
      case Or(left, right) =>
        formatBoolOp("OR", left, right)
      case Not(f) =>
        generateWhere(f).map(w => "(NOT " +: w :+ ")")
      /*
       According to some sources, newer versions of SAP
       support IN natively. Ours doesn't seem to. Anyhow,
       unrolling it into a bunch of equalities is
       more compatible.
       */
      case In(attr, values) =>
        val attemptedValues = values.map(formatBinOp("EQ", attr, _))
        val problems = attemptedValues.collect { case Left(s)           => s }
        val formattedValues = attemptedValues.collect { case Right(seq) => seq }

        if (problems.nonEmpty)
          Left(problems.mkString("; "))
        else if (formattedValues.isEmpty)
          Left("No valid values were supplied for IN")
        else {
          val ors = formattedValues.tail
            .foldLeft(formattedValues.head) {
              case (lparts, rparts) =>
                lparts ++ Seq(" OR ") ++ rparts
            }

          Right("(" +: ors :+ ")")
        }

      case f =>
        Left(s"Pushdown for $f is not supported.")
    }

    val whereClauseLines: PushdownResult = generateWhere(filter).flatMap { lines =>
      lines
        .find(_.length >= sapFilterLenLimit)
        .map(l => Left(s"$l is longer than $sapFilterLenLimit"))
        .getOrElse(Right(lines))
    }
  }

  def apply(filters: Array[Filter], schema: StructType): SapTableFilters = new SapTableFilters(filters, schema)

  def empty: SapTableFilters = new SapTableFilters(Array.empty, StructType(Seq.empty))
}
