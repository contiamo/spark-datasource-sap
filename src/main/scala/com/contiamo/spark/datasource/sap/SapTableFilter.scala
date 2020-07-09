package com.contiamo.spark.datasource.sap

import org.apache.spark.sql.sources._

case class SapTableFilter(filter: Filter, whereClauseLines: Seq[String])

object SapTableFilter {
  def apply(filter: Filter): SapTableFilter = {
    val whereClauseLines: Seq[String] = filter match {
      case IsNull(attr)    => Seq(s"$attr IS NULL")
      case IsNotNull(attr) => Seq(s"$attr IS NOT NULL")
      case _               => Seq.empty
    }
    SapTableFilter(filter, whereClauseLines)
  }

  def apply(filters: Array[Filter]): (Array[SapTableFilter], Array[Filter]) = {
    val (pushed, rejected) = filters
      .map(SapTableFilter(_))
      .partition { f =>
        f.whereClauseLines.nonEmpty && f.whereClauseLines.forall(_.length < 70)
      }

    (pushed, rejected.map(_.filter))
  }
}
