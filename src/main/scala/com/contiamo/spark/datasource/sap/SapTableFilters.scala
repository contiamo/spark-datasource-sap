package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.sources._

import scala.util.Try

class SapTableFilters(filters: Array[Filter]) {
  import SapTableFilters._

  val sapFilterLenLimit = 72

  private val (pushed_, rejected_) = filters
    .map(SapTableFilter)
    .partition { f =>
      f.whereClauseLines.nonEmpty && f.whereClauseLines.forall(_.length < sapFilterLenLimit)
    }

  val pushed: Array[Filter] = pushed_.map(_.filter)
  val rejected: Array[Filter] = rejected_.map(_.filter)

  val whereClauseLines: Seq[String] = pushed_.zipWithIndex.flatMap {
    case (f, 0) => f.whereClauseLines
    case (f, _) => " AND " +: f.whereClauseLines
  }
}

object SapTableFilters {
  protected def formatValue(x: Any): Option[String] =
    Try(org.apache.spark.sql.catalyst.expressions.Literal(x)).map(_.sql).toOption

  // TODO breakup long conditions
  protected def generateWhere(filter: Filter): Seq[String] = filter match {
    //case IsNull(attr) =>
      //Seq(s"($attr IS NULL)")
    //case IsNotNull(attr) =>
      //Seq(s"($attr IS NOT NULL)")
    case EqualTo(attr, value) =>
      formatValue(value).map(v => s"($attr EQ $v)").toSeq
    case Not(EqualTo(attr, value)) =>
      formatValue(value).map(v => s"($attr NE $v)").toSeq
    case LessThan(attr, value) =>
      formatValue(value).map(v => s"($attr LT $v)").toSeq
    case LessThanOrEqual(attr, value) =>
      formatValue(value).map(v => s"($attr LE $v)").toSeq
    case GreaterThan(attr, value) =>
      formatValue(value).map(v => s"($attr GT $v)").toSeq
    case GreaterThanOrEqual(attr, value) =>
      formatValue(value).map(v => s"($attr GE $v)").toSeq
    case And(left, right) =>
      val wl = generateWhere(left)
      val wr = generateWhere(right)
      if (wl.nonEmpty && wr.nonEmpty)
        ("(" +: wl) ++ Seq(" AND ") ++ (wr :+ ")")
      else
        Seq.empty
    case Or(left, right) =>
      val wl = generateWhere(left)
      val wr = generateWhere(right)
      if (wl.nonEmpty && wr.nonEmpty)
        ("(" +: wl) ++ Seq(" OR ") ++ (wr :+ ")")
      else
        Seq.empty
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
