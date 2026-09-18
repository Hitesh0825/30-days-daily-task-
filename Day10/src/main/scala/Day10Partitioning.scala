import org.apache.spark.sql.SparkSession

object Day10Partitioning {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 10 Partitioning")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ==========================================
    // 1. CREATE RDD WITH PARTITIONS
    // ==========================================

    println("\n===== ORIGINAL RDD =====")

    val numbers = sc.parallelize(1 to 20, 4)

    println("Numbers:")
    println(numbers.collect().mkString(", "))

    println("Number of partitions: " + numbers.getNumPartitions)


    // ==========================================
    // 2. VIEW DATA IN EACH PARTITION
    // ==========================================

    println("\n===== ORIGINAL PARTITIONS =====")

    val originalPartitions = numbers.mapPartitionsWithIndex {
      (index, iterator) =>
        Iterator(
          "Partition " + index + ": " +
            iterator.mkString(", ")
        )
    }

    originalPartitions.collect().foreach(println)


    // ==========================================
    // 3. REPARTITION
    // ==========================================

    println("\n===== REPARTITION =====")

    val repartitioned = numbers.repartition(2)

    println("Partitions after repartition: " +
      repartitioned.getNumPartitions)

    val repartitionedData = repartitioned.mapPartitionsWithIndex {
      (index, iterator) =>
        Iterator(
          "Partition " + index + ": " +
            iterator.mkString(", ")
        )
    }

    repartitionedData.collect().foreach(println)


    // ==========================================
    // 4. COALESCE
    // ==========================================

    println("\n===== COALESCE =====")

    val coalesced = numbers.coalesce(2)

    println("Partitions after coalesce: " +
      coalesced.getNumPartitions)

    val coalescedData = coalesced.mapPartitionsWithIndex {
      (index, iterator) =>
        Iterator(
          "Partition " + index + ": " +
            iterator.mkString(", ")
        )
    }

    coalescedData.collect().foreach(println)


    // ==========================================
    // 5. REPARTITION VS COALESCE
    // ==========================================

    println("\n===== REPARTITION VS COALESCE =====")

    println("repartition():")
    println("- Can increase or decrease partitions.")
    println("- Performs a full shuffle.")
    println("- Data is redistributed across partitions.")

    println("\ncoalesce():")
    println("- Normally used to reduce partitions.")
    println("- Avoids a full shuffle in the usual case.")
    println("- Combines existing partitions.")


    // ==========================================
    // 6. PARTITIONING AND PARALLELISM
    // ==========================================

    println("\n===== PARTITIONING AND PARALLELISM =====")

    println("A partition is a logical division of an RDD.")

    println("Each partition can be processed by a separate task.")

    println("More partitions can provide more parallelism,")
    println("but too many partitions can create scheduling overhead.")

    println("Current original partitions: " +
      numbers.getNumPartitions)


    // ==========================================
    // 7. PARTITION PROCESSING
    // ==========================================

    println("\n===== PARTITION PROCESSING =====")

    val partitionSums = numbers.mapPartitionsWithIndex {
      (index, iterator) =>

        val values = iterator.toList
        val sum = values.sum

        Iterator(
          "Partition " + index +
            " -> Values: " +
            values.mkString(", ") +
            " -> Sum: " +
            sum
        )
    }

    partitionSums.collect().foreach(println)


    // ==========================================
    // 8. PARTITIONING SUMMARY
    // ==========================================

    println("\n===== PARTITIONING SUMMARY =====")

    println("Original partitions: " +
      numbers.getNumPartitions)

    println("After repartition(2): " +
      repartitioned.getNumPartitions)

    println("After coalesce(2): " +
      coalesced.getNumPartitions)

    println("\nPartitioning controls how data is divided")
    println("and processed in Spark.")


    // ==========================================
    // 9. FINAL SUMMARY
    // ==========================================

    println("\n===== FINAL SUMMARY =====")

    println("Partitions divide an RDD into smaller logical parts.")
    println("Each partition can be processed by a Spark task.")
    println("repartition() redistributes data using shuffle.")
    println("coalesce() is generally used to reduce partitions.")
    println("Partitioning affects Spark parallelism and performance.")

    spark.stop()
  }
}
