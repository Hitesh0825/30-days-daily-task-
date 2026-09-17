import org.apache.spark.sql.SparkSession

object Day7LineageFaultTolerance {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 7 Lineage and Fault Tolerance")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ==========================================
    // 1. ORIGINAL RDD
    // ==========================================

    println("\n===== ORIGINAL RDD =====")

    val numbers = sc.parallelize(1 to 10, 2)

    println("Original numbers:")
    println(numbers.collect().mkString(", "))


    // ==========================================
    // 2. MULTI-STEP TRANSFORMATION CHAIN
    // ==========================================

    println("\n===== TRANSFORMATION CHAIN =====")

    val evenNumbers = numbers.filter(x => x % 2 == 0)

    val doubledNumbers = evenNumbers.map(x => x * 2)

    val finalRDD = doubledNumbers.filter(x => x > 5)

    println("Final result:")
    println(finalRDD.collect().mkString(", "))


    // ==========================================
    // 3. RDD LINEAGE
    // ==========================================

    println("\n===== RDD LINEAGE =====")

    println("Lineage:")
    println("numbers")
    println("   |")
    println("filter(x % 2 == 0)")
    println("   |")
    println("map(x * 2)")
    println("   |")
    println("filter(x > 5)")
    println("   |")
    println("collect()")

    println("\nSpark Lineage Information:")
    println(finalRDD.toDebugString)


    // ==========================================
    // 4. RDD IMMUTABILITY
    // ==========================================

    println("\n===== RDD IMMUTABILITY =====")

    println("Original RDD:")
    println(numbers.collect().mkString(", "))

    println("Transformed RDD:")
    println(evenNumbers.collect().mkString(", "))

    println("The original RDD is unchanged.")
    println("Transformations create new RDDs instead of modifying existing RDDs.")


    // ==========================================
    // 5. FAULT TOLERANCE
    // ==========================================

    println("\n===== FAULT TOLERANCE =====")

    println("RDDs store lineage information.")
    println("If a partition is lost, Spark can recompute that partition")
    println("using the transformations that created it.")

    println("\nExample:")
    println("numbers -> filter -> map -> filter -> finalRDD")

    println("If a finalRDD partition is lost:")
    println("1. Spark identifies the lost partition.")
    println("2. Spark checks the RDD lineage.")
    println("3. Spark recomputes only the required partition.")
    println("4. The complete dataset does not need to be recomputed.")


    // ==========================================
    // 6. CONCEPTUAL EXECUTOR LOSS SIMULATION
    // ==========================================

    println("\n===== EXECUTOR LOSS SIMULATION =====")

    println("Assume Executor 2 fails while processing a partition.")

    println("\nConceptual situation:")
    println("Partition 0 -> Executor 1 -> completed")
    println("Partition 1 -> Executor 2 -> LOST")
    println("Partition 2 -> Executor 1 -> completed")

    println("\nSpark response:")
    println("Partition 1 is recomputed from its lineage.")
    println("Partition 0 and Partition 2 do not need to be recomputed.")

    println("\nLineage used for the lost partition:")
    println("Original RDD")
    println("    -> filter")
    println("    -> map")
    println("    -> filter")
    println("    -> recompute lost partition")


    // ==========================================
    // 7. SUMMARY
    // ==========================================

    println("\n===== SUMMARY =====")

    println("RDDs are immutable.")
    println("Transformations create new RDDs.")
    println("RDD lineage records how an RDD was created.")
    println("Lineage provides fault tolerance.")
    println("Lost partitions can be recomputed from their parent RDDs.")
    println("Spark normally recomputes only the lost partition.")

    spark.stop()
  }
}
