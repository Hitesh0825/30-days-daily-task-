import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Day15-UDF").master("local[*]").getOrCreate()
    import spark.implicits._

    val salaryBand = udf((salary:Double) =>
      if(salary >= 100000) "HIGH"
      else if(salary >= 50000) "MEDIUM"
      else "LOW"
    )

    spark.udf.register("salary_band", (salary:Double) =>
      if(salary >= 100000) "HIGH"
      else if(salary >= 50000) "MEDIUM"
      else "LOW"
    )

    val employees = Seq(
      ("A",45000.0),
      ("B",75000.0),
      ("C",125000.0)
    ).toDF("name","salary")

    employees.withColumn("salary_band",salaryBand($"salary")).show()

    println("Built-in Spark functions are generally preferred over UDFs because Spark can optimize them better.")

    spark.stop()
  }
}
