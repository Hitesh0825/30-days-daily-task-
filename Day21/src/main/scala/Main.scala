import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day21-SparkCatalog")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")
    spark.sql("SHOW DATABASES").show(false)

    val bookings = Seq(
      (1,"Hyderabad","2026-10-01",2,5000),
      (2,"Delhi","2026-10-02",3,7500),
      (3,"Hyderabad","2026-10-03",1,2500)
    ).toDF(
      "booking_id",
      "city",
      "date",
      "nights",
      "revenue"
    )

    bookings.createOrReplaceTempView("hotel_bookings")

    spark.sql("""
      SELECT city,
             COUNT(*) AS bookings,
             SUM(revenue) AS revenue
      FROM hotel_bookings
      GROUP BY city
    """).show()

    spark.sql("DESCRIBE hotel_bookings").show(false)

    spark.stop()
  }
}
