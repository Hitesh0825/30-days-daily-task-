import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day19-BroadcastJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val transactions =
      spark.range(1,100001)
        .select(
          $"id".as("transaction_id"),
          (($"id" % 3) + 1).as("branch_id"),
          ($"id" * 10).as("amount")
        )

    val branches = Seq(
      (1,"Hyderabad"),
      (2,"Bengaluru"),
      (3,"Delhi")
    ).toDF("branch_id","branch")

    transactions
      .join(broadcast(branches), Seq("branch_id"))
      .groupBy("branch")
      .agg(
        count("*").as("transactions"),
        sum("amount").as("revenue")
      )
      .show()

    spark.stop()
  }
}
