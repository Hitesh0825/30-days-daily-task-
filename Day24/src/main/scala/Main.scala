import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Day24").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint/day24")
    val lines = ssc.socketTextStream("localhost", 9999)
    val pairs = lines.flatMap(_.split("\\s+")).map(word => (word, 1))
    val runningCounts = pairs.updateStateByKey[Int]((values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0)))
    runningCounts.print()
    println("Stateless = current batch only")
    println("Stateful = accumulated state across batches")
    ssc.start()
    ssc.awaitTermination()
  }
}
