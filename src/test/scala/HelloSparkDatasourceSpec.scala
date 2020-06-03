import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must

import scala.language.postfixOps
import com.contiamo.spark.datasource.sap.SapDataSource

class HelloSparkDatasourceSpec extends AnyFunSpec with SparkSessionTestWrapper with must.Matchers {

  val jcoOptions = Map.empty[String, String]

  def baseDF = spark.read
    .format("com.contiamo.spark.datasource.sap.SapDataSource")
    .options(jcoOptions)

  val username = jcoOptions("client.user")

  it("reads a SAP USR01 table") {
    val sourceDF =
      baseDF
        .option(SapDataSource.TABLE_KEY, "USR01")
        .load()

    val expectedCols = Seq("MANDT", "LANGU", "BNAME")
    sourceDF.schema.fields must contain allElementsOf expectedCols.map(col => StructField(col, StringType))

    sourceDF.select("BNAME").collect().map(_.mkString) must contain(username)
  }

  it("calls BAPI_USER_GET_DETAIL and retrieves its result set") {
    val sourceDF =
      baseDF
        .option(SapDataSource.BAPI_KEY, "BAPI_USER_GET_DETAIL")
        .option(SapDataSource.BAPI_ARGS_KEY, "{\"username\":\"" + username + "\"}")
        .option(SapDataSource.BAPI_OUTPUT_KEY, "IDENTITY")
        .load()

    sourceDF.printSchema()
    sourceDF.collect().foreach(r => println(r.toString()))

  }

  it("calls STFC_CONNECTION and retrieves its result set") {
    val sourceDF =
      baseDF
        .option(SapDataSource.BAPI_KEY, "STFC_CONNECTION")
        .option(SapDataSource.BAPI_ARGS_KEY, "{\"REQUTEXT\":\"hello " + username + "\"}")
        .option(SapDataSource.BAPI_OUTPUT_KEY, "ECHOTEXT")
        .load()

    sourceDF.printSchema()
    sourceDF.collect().foreach(r => println(r.toString()))
  }

  /* TODO don't forget to test
    - multiple table
    - different field types
 */
}
