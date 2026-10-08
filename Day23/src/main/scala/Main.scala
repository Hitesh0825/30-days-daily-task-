import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day23").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    val errors = lines.flatMap(_.split("\\s+")).filter(_ == "ERROR").map(_ => 1).reduce(_ + _)
    errors.print()
    println("Batch interval = 5 seconds")
    println("Start another terminal with: nc -lk 9999")
    ssc.start()
    ssc.awaitTermination()
  }
}
