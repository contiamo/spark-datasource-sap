package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.sources._

import scala.collection.mutable
import scala.util.Try

class SapTableFilters(filters: Array[Filter]) {
  import SapTableFilters._

  private val (pushed_, rejected_) = filters
    .map(SapTableFilter)
    .partition { f =>
      f.whereClauseLines.nonEmpty && f.whereClauseLines.forall(_.length < sapFilterLenLimit)
    }

  val pushed: Array[Filter] = pushed_.map(_.filter)
  val rejected: Array[Filter] = rejected_.map(_.filter)

  val whereClauseParts: Seq[String] = pushed_.zipWithIndex.flatMap {
    case (f, 0) => f.whereClauseLines
    case (f, _) => " AND " +: f.whereClauseLines
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
}

object SapTableFilters {
  val sapFilterLenLimit = 72

  protected def formatValue(x: Any): Seq[String] =
    Try(org.apache.spark.sql.catalyst.expressions.Literal(x)).map(_.sql).toOption.toSeq

  protected def formatBinOp(op: String, attr: String, value: Any): Seq[String] =
    formatValue(value).flatMap(v => Seq.empty :+ "(" :+ attr :+ s" $op " :+ v :+ ")")

  protected def formatBoolOp(op: String, left: Filter, right: Filter): Seq[String] = {
    val wl = generateWhere(left)
    val wr = generateWhere(right)
    if (wl.nonEmpty && wr.nonEmpty)
      ("(" +: wl) ++ Seq(s" $op ") ++ (wr :+ ")")
    else
      Seq.empty
  }

  // TODO breakup long conditions
  protected def generateWhere(filter: Filter): Seq[String] = filter match {
    /*
    It's impossible to reliably tell a NULL value
    from a bunch of zeroes in RFC_READ_TABLE output.
    Therefore its impossible to implement
    robust push-down for IS (NOT) NULL

    case IsNull(attr) =>
      Seq(s"($attr IS NULL)")
    case IsNotNull(attr) =>
      Seq(s"($attr IS NOT NULL)")
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
    case And(left, right) =>
      formatBoolOp("AND", left, right)
    case Or(left, right) =>
      formatBoolOp("OR", left, right)
    case Not(f) =>
      val w = generateWhere(f)
      if (w.nonEmpty)
        "(NOT " +: w :+ ")"
      else
        Seq.empty
    case _ =>
      Seq.empty
  }

  case class SapTableFilter(val filter: Filter) {
    val whereClauseLines: Seq[String] = generateWhere(filter)
  }

  def apply(filters: Array[Filter]): SapTableFilters = new SapTableFilters(filters)

  def empty: SapTableFilters = new SapTableFilters(Array.empty)
}
