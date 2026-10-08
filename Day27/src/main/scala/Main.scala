import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day27-Banking").master("local[*]").getOrCreate()
    import spark.implicits._

    val transactions = Seq(
      ("A1","B1",1000),
      ("A1","B1",2500),
      ("A1","B1",5000),
      ("A2","B2",400),
      ("A3","B2",700)
    ).toDF("account","branch_id","amount").persist(StorageLevel.MEMORY_AND_DISK)

    val risk = Seq(
      ("A1","HIGH"),
      ("A2","LOW"),
      ("A3","MEDIUM")
    ).toDF("account","risk")

    transactions.join(broadcast(risk),"account")
      .groupBy("account","risk")
      .agg(sum("amount").as("total_amount"),count("*").as("transaction_count"))
      .show()

    println("Banking streaming concepts: account aggregation, suspicious bursts, windows, broadcast reference data, persistence and partitioning.")
    println("YARN manages cluster resources for Spark applications.")
    spark.stop()
  }
}
