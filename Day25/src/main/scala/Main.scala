import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day25").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999)
    val transactions = lines.map(_ => ("transactions", 1))
    val rolling = transactions.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(600), Seconds(60))
    rolling.print()
    println("Batch interval = 10 seconds")
    println("Window length = 10 minutes")
    println("Sliding interval = 1 minute")
    ssc.start()
    ssc.awaitTermination()
  }
}
