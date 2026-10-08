import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day22").master("local[*]").getOrCreate()
    import spark.implicits._
    val tx = Seq((1,1,101,2,500),(2,2,102,1,900),(3,3,101,-1,500),(4,1,102,3,900)).toDF("transaction_id","customer_id","product_id","quantity","price")
    val customers = Seq((1,"Hitesh"),(2,"Ravi"),(3,"Neha")).toDF("customer_id","customer")
    val products = Seq((101,"Laptop"),(102,"Phone")).toDF("product_id","product")
    val clean = tx.filter($"quantity" > 0).join(customers,"customer_id").join(products,"product_id").withColumn("revenue",$"quantity"*$"price")
    clean.show()
    clean.groupBy("product").agg(sum("revenue").as("revenue"),count("*").as("transactions")).show()
    clean.write.mode("overwrite").partitionBy("product").parquet("output/day22_sales")
    spark.stop()
  }
}
