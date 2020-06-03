import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must

import scala.language.postfixOps


class HelloSparkDatasourceSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with must.Matchers {
  
  val sourceDF = spark.read
    .format("com.contiamo.spark.datasource.sap.SapDataSource")
    .option("mockdata", "foo,hello;bar,hello")
    .load()

  it("reads a custom datasource") {
    val expectedData = Seq(Row("foo", "hello"), Row("bar", "hello"))

    sourceDF.collect() mustEqual expectedData
  }
}
