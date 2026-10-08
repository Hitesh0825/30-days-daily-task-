import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day29-ECommerce").master("local[*]").getOrCreate()
    import spark.implicits._

    val raw = Seq(
      (1,1,"Laptop",2,1000),
      (2,2,"Phone",1,500),
      (3,1,"Laptop",-1,1000),
      (4,3,"Tablet",3,700)
    ).toDF("id","customer_id","product","quantity","price")

    val clean = raw.filter($"quantity" > 0)
      .withColumn("revenue",$"quantity" * $"price")
      .cache()

    println("Cleaned data")
    clean.show()

    clean.groupBy("product")
      .agg(sum("revenue").as("total_revenue"),count("*").as("orders"))
      .show()

    val w = Window.partitionBy("customer_id").orderBy($"revenue".desc)

    clean.withColumn("customer_rank",row_number().over(w)).show()

    println("Architecture: RAW -> CLEAN -> ENRICH -> JOIN -> AGGREGATE -> WINDOW -> OUTPUT")
    println("RDD/Pair RDD can handle low-level transformations.")
    println("DataFrames/Datasets use Spark SQL optimization.")
    println("Use UDF only when built-in Spark functions are insufficient.")
    spark.stop()
  }
}
