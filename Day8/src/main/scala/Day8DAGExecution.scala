import org.apache.spark.sql.SparkSession

object Day8DAGExecution {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 8 DAG and Spark Execution")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ==========================================
    // 1. CREATE INPUT RDD
    // ==========================================

    println("\n===== INPUT RDD =====")

    val numbers = sc.parallelize(1 to 20, 4)

    println("Input numbers:")
    println(numbers.collect().mkString(", "))

    println("Number of partitions: " + numbers.getNumPartitions)


    // ==========================================
    // 2. NARROW TRANSFORMATIONS
    // ==========================================

    println("\n===== NARROW TRANSFORMATIONS =====")

    val evenNumbers = numbers.filter(x => x % 2 == 0)

    val doubledNumbers = evenNumbers.map(x => x * 2)

    println("After filter and map:")
    println(doubledNumbers.collect().mkString(", "))


    // ==========================================
    // 3. CREATE PAIR RDD
    // ==========================================

    println("\n===== PAIR RDD =====")

    val pairs = doubledNumbers.map(x => (x % 3, x))

    println("Key-value pairs:")
    println(pairs.collect().mkString(", "))


    // ==========================================
    // 4. WIDE TRANSFORMATION - reduceByKey
    // ==========================================

    println("\n===== REDUCE BY KEY =====")

    val reduced = pairs.reduceByKey((a, b) => a + b)

    println("Reduced result:")
    println(reduced.collect().mkString(", "))


    // ==========================================
    // 5. ANOTHER TRANSFORMATION
    // ==========================================

    println("\n===== FINAL TRANSFORMATION =====")

    val finalResult = reduced
      .mapValues(x => x * 10)
      .sortByKey()

    println("Final result:")
    println(finalResult.collect().mkString(", "))


    // ==========================================
    // 6. DAG / LINEAGE
    // ==========================================

    println("\n===== DAG / LINEAGE =====")

    println(
      """
numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey  <-- SHUFFLE
   |
mapValues
   |
sortByKey    <-- SHUFFLE
   |
collect()
"""
    )

    println("RDD Debug Information:")
    println(finalResult.toDebugString)


    // ==========================================
    // 7. STAGE AND SHUFFLE EXPLANATION
    // ==========================================

    println("\n===== STAGES AND SHUFFLE =====")

    println("filter and map are narrow transformations.")

    println("reduceByKey is a wide transformation.")
    println("reduceByKey creates a shuffle boundary.")

    println("sortByKey is also a wide transformation.")
    println("sortByKey creates another shuffle boundary.")

    println("\nConceptual stage flow:")

    println("Stage 1:")
    println("numbers -> filter -> map -> map to Pair RDD")

    println("Stage 2:")
    println("reduceByKey")

    println("Stage 3:")
    println("mapValues -> sortByKey")


    // ==========================================
    // 8. JOBS, STAGES, TASKS AND PARTITIONS
    // ==========================================

    println("\n===== JOBS, STAGES, TASKS AND PARTITIONS =====")

    println("Job:")
    println("A job is created when an action such as collect() is executed.")

    println("\nStage:")
    println("A stage is a group of operations that can be executed without a shuffle.")

    println("\nTask:")
    println("A task is the unit of work performed for one partition.")

    println("\nPartition:")
    println("A partition is a logical division of an RDD.")

    println("\nRelationship:")
    println("Job -> Stages -> Tasks -> Partitions")


    // ==========================================
    // 9. NARROW VS WIDE TRANSFORMATIONS
    // ==========================================

    println("\n===== NARROW VS WIDE =====")

    println("Narrow transformations:")
    println("- filter")
    println("- map")
    println("- mapValues")

    println("\nWide transformations:")
    println("- reduceByKey")
    println("- sortByKey")

    println("\nWide transformations require data to be shuffled between partitions.")


    // ==========================================
    // 10. STAGE PREDICTION SCENARIO
    // ==========================================

    println("\n===== STAGE PREDICTION =====")

    println("Pipeline:")
    println("map -> filter -> reduceByKey -> mapValues -> collect")

    println("\nConceptual stages:")

    println("Stage 1:")
    println("map -> filter")

    println("Stage 2:")
    println("reduceByKey -> mapValues")

    println("\nExpected number of stages: 2")

    println("The shuffle created by reduceByKey separates the two stages.")


    // ==========================================
    // 11. SUMMARY
    // ==========================================

    println("\n===== SUMMARY =====")

    println("Spark creates a DAG from transformations and actions.")
    println("Narrow transformations do not require a shuffle.")
    println("Wide transformations create shuffle boundaries.")
    println("A job is triggered by an action.")
    println("Stages are separated by shuffle boundaries.")
    println("Tasks operate on partitions.")
    println("reduceByKey is a wide transformation.")
    println("sortByKey is a wide transformation.")

    spark.stop()
  }
}
