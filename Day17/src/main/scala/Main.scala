import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day17-WindowFunctions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val students = Seq(
      ("Asha","Math",91),
      ("Ravi","Math",85),
      ("Neha","Math",88),
      ("Kiran","Math",91),
      ("Asha","AI",95),
      ("Ravi","AI",90)
    ).toDF("student","course","marks")

    val w = Window.partitionBy("course").orderBy($"marks".desc)

    students
      .withColumn("row_number", row_number().over(w))
      .withColumn("rank", rank().over(w))
      .withColumn("dense_rank", dense_rank().over(w))
      .show()

    val latest = Seq(
      ("C1","2026-10-01","ACTIVE"),
      ("C1","2026-10-05","BLOCKED"),
      ("C2","2026-10-02","ACTIVE")
    ).toDF("customer","date","status")

    latest
      .withColumn(
        "rn",
        row_number().over(
          Window.partitionBy("customer").orderBy($"date".desc)
        )
      )
      .filter($"rn" === 1)
      .show()

    spark.stop()
  }
}
