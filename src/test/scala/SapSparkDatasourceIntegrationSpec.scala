import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must

import scala.language.postfixOps
import com.contiamo.spark.datasource.sap.{SapDataSource, SapDataSourceReader, SapSparkDestinationDataProvider}
import com.sap.conn.jco.JCoDestinationManager
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Column

import scala.collection.JavaConverters._
import scala.util.Try

class SapSparkDatasourceIntegrationSpec extends AnyFunSpec with SparkSessionTestWrapper with must.Matchers {
  import spark.implicits._
  import org.apache.spark.sql.functions.col

  private val conf = ConfigFactory.load.getConfig("spark-sap-test")
  private val jcoClienConf = conf.getConfig("jco.client")
  private val jcoOptions = jcoClienConf.root.keySet.asScala
    .map(k => s"jco.client.$k" -> jcoClienConf.getString(k))
    .toMap

  /* warm-up JCo connection in case the SAP system needs
     time before its starts accepting connections.
   */
  private val jcoDestKey = SapSparkDestinationDataProvider.register(SapDataSourceReader.extractJcoOptions(jcoOptions))
  private val jcoDest = JCoDestinationManager.getDestination(jcoDestKey)
  Try(jcoDest.ping())

  val format = classOf[SapDataSource].getName

  def baseDF =
    spark.read
      .format(format)
      .options(jcoOptions)

  val username = jcoOptions("jco.client.user")

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] =
    schema.fields.flatMap { f =>
      val colName =
        if (prefix == null) f.name
        else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _              => Array(col(colName).alias(colName))
      }
    }

  private def userGetDetailCall(options: Map[String, String] = Map.empty) =
    baseDF
      .option(SapDataSource.BAPI_KEY, "BAPI_USER_GET_DETAIL")
      .option(SapDataSource.BAPI_ARGS_KEY, "{\"USERNAME\":\"" + username + "\"}")
      .options(options)
      .load()

  it("reads a SAP USR01 table") {
    val sourceDF =
      baseDF
        .option(SapDataSource.TABLE_KEY, "USR01")
        .load()

    val expectedCols = Seq("MANDT", "LANGU", "BNAME")
    sourceDF.schema.fields must contain allElementsOf expectedCols.map(col => StructField(col, StringType))

    sourceDF.collect().map(_.mkString).mkString must include(username)

    sourceDF.select("BNAME").collect().map(_.mkString) must contain(username)

    // not using spark.count to force pushdown of both columns
    sourceDF.select("MANDT").where($"BNAME" === username).collect().length mustEqual 1
  }

  it("lists schemas for multiple tables") {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats

    val sourceDF =
      baseDF
        .option(SapDataSource.LIST_TABLES_KEY, """["USR01", "DD02L", "TFDIR"]""")
        .load()

    val expectedCols = Seq("name", "schemaJson", "dfOptions")
    sourceDF.schema.fields must contain allElementsOf expectedCols.map(StructField(_, StringType))

    val res = sourceDF.collect()
    res.map(_.getString(0)) mustEqual Seq("USR01", "DD02L", "TFDIR")
    res.foreach { row =>
      val repotedSchema = DataType.fromJson(row.getString(1))
      val dfOptions = parse(row.getString(2)).extract[Map[String, String]]

      spark.read
        .format(format)
        .options(dfOptions)
        .load()
        .schema mustEqual repotedSchema
    }
  }

  describe("BAPI partition reader") {
    it("calls STFC_CONNECTION and retrieves its export parameters") {
      val sourceDF =
        baseDF
          .option(SapDataSource.BAPI_KEY, "STFC_CONNECTION")
          .option(SapDataSource.BAPI_ARGS_KEY, "{\"REQUTEXT\":\"hello " + username + "\"}")
          .load()

      val res = sourceDF.collect()
      res.length mustEqual 1
      res.head.schema.fieldNames must contain allElementsOf Seq("ECHOTEXT", "ECHOTEXT")
      res.head.length mustEqual 2
      res.head.get(0) mustEqual s"hello $username"
    }

    it("calls RFC_READ_TABLE on USR01 with FIELDS table parameter") {
      val argsJson =
        """{
          | "QUERY_TABLE" : "USR01",
          | "FIELDS" : [
          |   {"FIELDNAME": "BNAME"}
          | ]
          |}
          |""".stripMargin
      val sourceDF =
        baseDF
          .option(SapDataSource.BAPI_KEY, "RFC_READ_TABLE")
          .option(SapDataSource.BAPI_ARGS_KEY, argsJson)
          .option(SapDataSource.BAPI_OUTPUT_TABLE_KEY, "DATA")
          .load()

      // if parameters were passed correctly then the result must contain usernames only
      // otherwisw each elemen will be concatenation of columns and != username
      sourceDF.collect().map(_.mkString) must contain(username)
    }

    describe("calls BAPI_USER_GET_DETAIL and") {
      it("retrieves its export parameters") {
        val sourceDF = userGetDetailCall()

        val expectedCols = Seq(
          "ADDRESS",
          "ADMINDATA",
          "ALIAS",
          "COMPANY",
          "DEFAULTS",
          "IDENTITY",
          "ISLOCKED",
          "LASTMODIFIED",
          "LOGONDATA",
          "REF_USER"
        )

        val expectedSubCols = Seq(
          "LASTMODIFIED.MODDATE",
          "LASTMODIFIED.MODTIME",
          "ADDRESS.FIRSTNAME",
          "ADDRESS.LASTNAME"
        )

        val bapiSchema = sourceDF.schema
        bapiSchema.fields.map(_.name) must contain allElementsOf expectedCols
        val flatDf = sourceDF.select(flattenSchema(bapiSchema): _*)

        // we can't quite control what's in the output
        // but we can assert that the result is non-trivial
        val res = flatDf.collect()
        res.length mustBe 1
        res.head.mkString("") must not equal ("")

        val flatSchema = res.head.schema
        // assert that the result set contains fields of non-trivial types
        // .collect() forces us do deserialize those values
        flatSchema.fields.map(_.dataType).count(_ != StringType) mustBe >(0)
        flatSchema.fields.map(_.name) must contain allElementsOf expectedSubCols
      }

      it("retrieves a subset of export parameters") {
        val sourceDF = userGetDetailCall()

        val expectedCols = Seq("ADDRESS", "LASTMODIFIED")
        val expectedSubCols = Seq(
          "LASTMODIFIED.MODDATE",
          "LASTMODIFIED.MODTIME",
          "ADDRESS.FIRSTNAME",
          "ADDRESS.LASTNAME"
        )

        val fullResStr = sourceDF.collect().head.mkString

        for (expCols <- Seq(expectedCols, expectedSubCols)) {
          val subsetDF = sourceDF.select(expCols.map(c => col(c).alias(c)): _*)
          val res = subsetDF.collect()

          res.length mustBe 1
          res.head.schema.fieldNames must contain theSameElementsAs expCols
          res.head.schema.fieldNames.foreach { fn =>
            fullResStr must include(res.head.getAs[Any](fn).toString)
          }
        }
      }

      it("retrieves flattened export parameters") {
        val nestedDF = userGetDetailCall()
        val flattenedDF = userGetDetailCall(Map(SapDataSource.BAPI_FLATTEN_KEY -> "true"))

        val flatBapiSchema = flattenedDF.schema
        val nestedSchema = nestedDF.schema
        val flatNestedRes = nestedDF.select(flattenSchema(nestedSchema): _*).collect()
        val flatNestedSchema = flatNestedRes.head.schema

        flatBapiSchema.fields.map(_.name) must contain theSameElementsAs flatNestedSchema.fields.map {
          _.name.replace('.', '_')
        }
        // no un-flattened columns are left in the payload
        flatBapiSchema.fields.filter(_.dataType.isInstanceOf[StructType]) must be(empty)
        // there are non-trivial types in the flattened payload
        flatBapiSchema.fields.filterNot(_.dataType == StringType) must not be (empty)

        val res = flattenedDF.collect()
        res.length mustBe 1
        res.head.mkString("") must not equal ("")

        flatNestedRes mustEqual res
      }

      it("retrieves a subset of flattened export parameters") {
        val sourceDF = userGetDetailCall(Map(SapDataSource.BAPI_FLATTEN_KEY -> "true"))

        val expectedSubCols = Seq(
          "LASTMODIFIED_MODDATE",
          "LASTMODIFIED_MODTIME",
          "ADDRESS_FIRSTNAME",
          "ADDRESS_LASTNAME"
        )

        val fullResStr = sourceDF.collect().head.mkString

        val subsetDF = sourceDF.select(expectedSubCols.map(c => col(c).alias(c)): _*)
        val res = subsetDF.collect()

        res.length mustBe 1
        res.head.schema.fieldNames must contain theSameElementsAs expectedSubCols
        res.head.schema.fieldNames.foreach { fn =>
          fullResStr must include(res.head.getAs[Any](fn).toString)
        }
      }

      it("retrieves its table parameter") {
        val sourceDF = userGetDetailCall(Map(SapDataSource.BAPI_OUTPUT_TABLE_KEY -> "PROFILES"))

        val expectedCols = Seq("BAPIPROF", "BAPIPTEXT", "BAPITYPE", "BAPIAKTPS")

        val res = sourceDF.collect()
        res.length mustBe >=(1)
        res.head.schema.fields.map(_.name) must contain allElementsOf expectedCols
      }

    }
  }

  /* TODO don't forget to test
    - multiple table
    - assert projection pushdowns
 */
}
