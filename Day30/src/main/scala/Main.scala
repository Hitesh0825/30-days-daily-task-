import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day30-FinalCapstone").master("local[*]").getOrCreate()
    import spark.implicits._

    val transactions = Seq(
      (1,"C1","Laptop",2,1000),
      (2,"C2","Phone",1,700),
      (3,"C1","Mouse",3,50),
      (4,"C3","Laptop",1,1000),
      (5,"C2","Phone",-1,700)
    ).toDF("transaction_id","customer_id","product","quantity","price")

    val badRecords = spark.sparkContext.longAccumulator("Bad Records")

    val cleaned = transactions.rdd.filter { row =>
      val q = row.getAs[Int]("quantity")
      if (q <= 0) { badRecords.add(1); false } else true
    }.toDF().persist(StorageLevel.MEMORY_AND_DISK)

    println("Bad records = " + badRecords.value)

    val productMaster = Seq(
      ("Laptop","Electronics"),
      ("Phone","Electronics"),
      ("Mouse","Accessories")
    ).toDF("product","category")

    val enriched = cleaned
      .join(broadcast(productMaster),"product")
      .withColumn("revenue",$"quantity" * $"price")

    enriched.show()

    val report = enriched.groupBy("category").agg(
      sum("revenue").as("total_revenue"),
      avg("revenue").as("average_revenue"),
      count("*").as("transactions")
    )

    report.show()

    val w = Window.partitionBy("customer_id").orderBy($"revenue".desc)
    val ranked = enriched.withColumn("customer_rank",row_number().over(w))
    ranked.show()

    val riskUdf = udf((revenue: Double) => if (revenue >= 1500) "HIGH" else if (revenue >= 500) "MEDIUM" else "LOW")

    val finalReport = ranked.withColumn("risk_category",riskUdf($"revenue"))
    finalReport.show()

    finalReport.createOrReplaceTempView("transactions")
    spark.sql("SELECT category, risk_category, SUM(revenue) AS revenue FROM transactions GROUP BY category, risk_category ORDER BY revenue DESC").show()

    finalReport.repartition($"category").write.mode("overwrite").parquet("output/day30_capstone")

    println("DAG, lineage, stages, executors and YARN are key Spark execution concepts.")
    spark.stop()
  }
}
