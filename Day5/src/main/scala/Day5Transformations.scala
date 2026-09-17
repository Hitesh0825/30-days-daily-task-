import org.apache.spark.sql.SparkSession

object Day5Transformations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 5 Transformations and Actions")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    // ============================================================
    // 1. MAP TRANSFORMATION
    // ============================================================

    println("\n===== MAP =====")

    val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

    val squaredNumbers = numbers.map(x => x * x)

    println("Original numbers:")
    println(numbers.collect().mkString(", "))

    println("Squared numbers:")
    println(squaredNumbers.collect().mkString(", "))


    // ============================================================
    // 2. FILTER TRANSFORMATION
    // ============================================================

    println("\n===== FILTER =====")

    val evenNumbers = numbers.filter(x => x % 2 == 0)

    println("Even numbers:")
    println(evenNumbers.collect().mkString(", "))


    // ============================================================
    // 3. FLATMAP TRANSFORMATION
    // ============================================================

    println("\n===== FLATMAP =====")

    val sentences = sc.parallelize(
      List(
        "Spark is fast",
        "Scala is powerful",
        "Big data is interesting"
      )
    )

    val words = sentences.flatMap(sentence => sentence.split(" "))

    println("Words:")
    println(words.collect().mkString(", "))


    // ============================================================
    // 4. DISTINCT TRANSFORMATION
    // ============================================================

    println("\n===== DISTINCT =====")

    val duplicateNumbers =
      sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 5, 5))

    val uniqueNumbers = duplicateNumbers.distinct()

    println("Original:")
    println(duplicateNumbers.collect().mkString(", "))

    println("Distinct:")
    println(uniqueNumbers.collect().mkString(", "))


    // ============================================================
    // 5. UNION TRANSFORMATION
    // ============================================================

    println("\n===== UNION =====")

    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List(4, 5, 6))

    val combinedRDD = rdd1.union(rdd2)

    println("RDD 1:")
    println(rdd1.collect().mkString(", "))

    println("RDD 2:")
    println(rdd2.collect().mkString(", "))

    println("Union:")
    println(combinedRDD.collect().mkString(", "))


    // ============================================================
    // ACTIONS
    // ============================================================

    println("\n===== ACTIONS =====")


    // COUNT

    println("\n--- COUNT ---")

    val count = numbers.count()

    println(s"Number of elements: $count")


    // COLLECT

    println("\n--- COLLECT ---")

    val collected = numbers.collect()

    println("Collected elements:")
    println(collected.mkString(", "))


    // FIRST

    println("\n--- FIRST ---")

    val firstElement = numbers.first()

    println(s"First element: $firstElement")


    // TAKE

    println("\n--- TAKE ---")

    val firstThree = numbers.take(3)

    println("First 3 elements:")
    println(firstThree.mkString(", "))


    // REDUCE

    println("\n--- REDUCE ---")

    val sum = numbers.reduce((a, b) => a + b)

    println(s"Sum: $sum")


    // ============================================================
    // LOG ANALYZER
    // ============================================================

    println("\n========================================")
    println("        LOG ANALYZER")
    println("========================================")

    val logs = sc.parallelize(
      List(
        "INFO User login successful",
        "ERROR Database connection failed",
        "INFO User viewed dashboard",
        "ERROR File not found",
        "WARNING Disk space is low",
        "ERROR Authentication failed",
        "INFO User logout successful",
        "ERROR Database timeout",
        "INFO Application started"
      )
    )

    println("\nAll Logs:")

    logs.collect().foreach(println)


    // Filter ERROR messages

    val errorLogs = logs.filter(log => log.contains("ERROR"))

    println("\nERROR Logs:")

    errorLogs.collect().foreach(println)


    // Count ERROR messages

    val errorCount = errorLogs.count()

    println(s"\nTotal ERROR messages: $errorCount")


    // ============================================================
    // WORD COUNT USING FLATMAP
    // ============================================================

    println("\n===== WORD COUNT =====")

    val logWords = logs.flatMap(log => log.split(" "))

    println("Total words:")
    println(logWords.count())


    // ============================================================
    // STOP SPARK
    // ============================================================

    spark.stop()
  }
}
