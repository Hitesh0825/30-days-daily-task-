import org.apache.spark.sql.SparkSession

object Day6WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 6 Word Count")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ==========================================
    // 1. CLASSIC WORD COUNT
    // ==========================================

    println("\n===== CLASSIC WORD COUNT =====")

    val text = sc.parallelize(
      List(
        "Spark is fast",
        "Spark is powerful",
        "Scala is powerful"
      )
    )

    val classicWordCount = text
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    classicWordCount.collect()
      .sortBy(_._1)
      .foreach(println)


    // ==========================================
    // 2. CASE-INSENSITIVE WORD COUNT
    //    IGNORE PUNCTUATION AND EMPTY WORDS
    // ==========================================

    println("\n===== CASE-INSENSITIVE WORD COUNT =====")

    val applicationLogs = sc.parallelize(
      List(
        "ERROR Spark failed to start!",
        "error Spark failed again.",
        "INFO Application started successfully.",
        "WARNING Spark memory is low!",
        "ERROR Application stopped.",
        "info Spark application running.",
        "ERROR spark failed to connect!",
        "Spark is fast, Spark is powerful."
      )
    )

    val cleanWordCount = applicationLogs
      .flatMap(line => line.split("\\s+"))
      .map(word => word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase)
      .filter(word => word.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    cleanWordCount
      .collect()
      .sortBy(-_._2)
      .foreach(println)


    // ==========================================
    // 3. TOP 10 MOST FREQUENT WORDS
    // ==========================================

    println("\n===== TOP 10 MOST FREQUENT WORDS =====")

    val top10Words = cleanWordCount
      .takeOrdered(10)(Ordering.by[(String, Int), Int](-_._2))

    top10Words.foreach {
      case (word, count) =>
        println(s"$word -> $count")
    }


    // ==========================================
    // 4. EXPLANATION
    // ==========================================

    println("\n===== TRANSFORMATION FLOW =====")
    println("flatMap -> Splits lines into individual words")
    println("map -> Converts each word into (word, 1)")
    println("reduceByKey -> Adds counts for the same word")
    println("filter -> Removes empty words")
    println("toLowerCase -> Makes counting case-insensitive")


    spark.stop()
  }
}
