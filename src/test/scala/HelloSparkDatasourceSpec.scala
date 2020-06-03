import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must

import scala.language.postfixOps

class HelloSparkDatasourceSpec extends AnyFunSpec with SparkSessionTestWrapper with must.Matchers {

  val jcoOptions = Map.empty[String, String]

  val sourceDF = spark.read
    .format("com.contiamo.spark.datasource.sap.SapDataSource")
    .options(jcoOptions)
    .option(DataSourceOptions.TABLE_KEY, "USR01")
    .load()

  it("reads a custom datasource") {
    val expectedCols = Seq("MANDT", "LANGU", "BNAME")
    sourceDF.schema.fields must contain allElementsOf expectedCols.map(col => StructField(col, StringType))

    sourceDF.select("BNAME").collect().map(_.mkString) must contain(jcoOptions("client.user"))
  }

  /* TODO don't forget to test
    - multiple table
    - different field types
 */
}
