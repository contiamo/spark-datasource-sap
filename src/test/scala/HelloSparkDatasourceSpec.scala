import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must

import scala.language.postfixOps
import com.contiamo.spark.datasource.sap.SapDataSource
import org.apache.spark.sql.Column

class HelloSparkDatasourceSpec extends AnyFunSpec with SparkSessionTestWrapper with must.Matchers {
  import spark.implicits._
  import org.apache.spark.sql.functions.col

  val jcoOptions = Map.empty[String, String]

  def baseDF =
    spark.read
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

    sourceDF.collect().map(_.mkString).mkString must include(username)

    sourceDF.select("BNAME").collect().map(_.mkString) must contain(username)

    // not using sparkCount to force pushdown of both columns
    sourceDF.select("MANDT").where($"BNAME" === username).collectAsList().size() mustBe 1
  }

  def flattenSchema(schema: StructType, prefix: String = null): Array[Column] =
    schema.fields.flatMap { f =>
      val colName =
        if (prefix == null) f.name
        else (prefix + "." + f.name)

      val colAlias = colName.replaceAll("\\.", "_")

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _              => Array(col(colName).alias(colAlias))
      }
    }

  it("calls BAPI_USER_GET_DETAIL and retrieves its result set") {
    val sourceDF =
      baseDF
        .option(SapDataSource.BAPI_KEY, "BAPI_USER_GET_DETAIL")
        .option(SapDataSource.BAPI_ARGS_KEY, "{\"username\":\"" + username + "\"}")
        //option(SapDataSource.BAPI_OUTPUT_KEY, "PROFILES")
        .load()

    val df1 = sourceDF.select(flattenSchema(sourceDF.schema):_*)
    df1.printSchema
    df1.collect().foreach(r => println(r.toString()))
  }

  it("calls STFC_CONNECTION and retrieves its result set") {
    val sourceDF =
      baseDF
        .option(SapDataSource.BAPI_KEY, "STFC_CONNECTION")
        .option(SapDataSource.BAPI_ARGS_KEY, "{\"REQUTEXT\":\"hello " + username + "\"}")
        .load()

    sourceDF.printSchema()
    sourceDF.collect().foreach(r => println(r.toString()))
  }

  /* TODO don't forget to test
    - multiple table
    - different field types
 */
}
