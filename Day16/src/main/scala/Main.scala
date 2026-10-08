import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Day16-Aggregations").master("local[*]").getOrCreate()
    import spark.implicits._

    val employees = Seq(
      ("IT",60000.0),
      ("IT",80000.0),
      ("HR",50000.0),
      ("HR",55000.0),
      ("Sales",70000.0)
    ).toDF("department","salary")

    employees
      .groupBy("department")
      .agg(
        count("*").as("employees"),
        sum("salary").as("total_salary"),
        avg("salary").as("avg_salary"),
        min("salary").as("min_salary"),
        max("salary").as("max_salary")
      )
      .filter($"avg_salary" > 55000)
      .show()

    spark.stop()
  }
}
