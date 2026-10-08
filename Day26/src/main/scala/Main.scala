import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day26").master("local[*]").getOrCreate()
    import spark.implicits._
    val thresholds = spark.sparkContext.broadcast(Map("heart_rate" -> 120.0, "temperature" -> 39.0, "spo2" -> 90.0))
    val vitals = Seq((1,"heart_rate",135.0),(2,"heart_rate",80.0),(1,"temperature",39.5),(3,"spo2",88.0)).toDF("patient_id","vital","value")
    val alerts = vitals.filter(($"vital" === "heart_rate" && $"value" > thresholds.value("heart_rate")) || ($"vital" === "temperature" && $"value" > thresholds.value("temperature")) || ($"vital" === "spo2" && $"value" < thresholds.value("spo2")))
    alerts.withColumn("alert",lit("ABNORMAL")).show()
    println("Broadcast thresholds used")
    println("Accumulators can count abnormal records")
    println("Window/stateful processing can detect repeated abnormal readings")
    println("Partitions, DAG and fault tolerance are part of the design")
    spark.stop()
  }
}
