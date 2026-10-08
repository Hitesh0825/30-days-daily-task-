import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Day28-Booking").master("local[*]").getOrCreate()
    import spark.implicits._

    val capacity = spark.sparkContext.broadcast(Map("HOTEL1" -> 100,"HOTEL2" -> 80))

    val events = Seq(
      ("HOTEL1","BOOK",20),
      ("HOTEL1","BOOK",10),
      ("HOTEL1","CANCEL",5),
      ("HOTEL2","BOOK",30)
    ).toDF("hotel","event","rooms")

    events.groupBy("hotel","event").agg(sum("rooms").as("rooms")).show()

    println("Booking and cancellation state can be maintained with stateful streaming.")
    println("Window operations calculate recent booking activity.")
    println("Broadcast reference data stores hotel capacity.")
    println("Spark SQL can generate booking reports.")
    println("Hotel 1 capacity = " + capacity.value("HOTEL1"))
    spark.stop()
  }
}
