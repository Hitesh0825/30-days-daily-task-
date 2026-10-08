import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day18-Joins")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val customers = Seq(
      (1,"Hitesh"),
      (2,"Ravi"),
      (3,"Neha")
    ).toDF("customer_id","name")

    val orders = Seq(
      (101,1,500),
      (102,2,800),
      (103,4,300)
    ).toDF("order_id","customer_id","amount")

    val payments = Seq(
      (101,"PAID"),
      (102,"PENDING")
    ).toDF("order_id","status")

    orders.join(customers, Seq("customer_id"), "inner").show()
    orders.join(customers, Seq("customer_id"), "left").show()
    orders.join(customers, Seq("customer_id"), "right").show()
    orders.join(customers, Seq("customer_id"), "full").show()

    orders.as("o")
      .join(customers.as("c"),
        $"o.customer_id" === $"c.customer_id",
        "left")
      .join(payments.as("p"),
        $"o.order_id" === $"p.order_id",
        "left")
      .select(
        $"o.order_id",
        $"c.name",
        $"o.amount",
        coalesce($"p.status",lit("NOT_PAID")).as("payment_status")
      )
      .show()

    spark.stop()
  }
}
