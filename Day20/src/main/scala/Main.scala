import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day20-FileFormats")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sales = Seq(
      ("2026-10-01","Hyderabad",1000),
      ("2026-10-02","Hyderabad",1500),
      ("2026-10-02","Delhi",1200)
    ).toDF("date","city","amount")

    val df = sales
      .withColumn("year",year($"date"))
      .withColumn("month",month($"date"))
      .withColumn("day",dayofmonth($"date"))

    df.write.mode("overwrite").option("header","true").csv("output/csv")
    df.write.mode("overwrite").json("output/json")
    df.write.mode("overwrite").parquet("output/parquet")

    df.write
      .mode("overwrite")
      .partitionBy("year","month","day")
      .parquet("output/daily_sales")

    println("CSV, JSON and Parquet completed.")

    spark.stop()
  }
}
