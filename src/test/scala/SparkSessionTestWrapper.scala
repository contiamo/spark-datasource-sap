import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("spark test")
      .getOrCreate()
}
