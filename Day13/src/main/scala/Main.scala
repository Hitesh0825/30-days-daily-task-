import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day13-SparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._

    val customers = Seq(
      (1,"Hitesh",25,"Hyderabad"),
      (2,"Rahul",30,"Delhi"),
      (3,"Aman",22,"Hyderabad")
    ).toDF("id","name","age","city")

    customers.printSchema()
    customers.select("name","city").show()
    customers.filter($"age" >= 25).show()

    val result = customers.withColumn(
      "age_group",
      when($"age" >= 30,"30+").otherwise("Under 30")
    )
    result.show()

    result.createOrReplaceTempView("customers")
    spark.sql("""
      SELECT city, COUNT(*) AS customer_count
      FROM customers
      GROUP BY city
    """).show()

    spark.stop()
  }
}
