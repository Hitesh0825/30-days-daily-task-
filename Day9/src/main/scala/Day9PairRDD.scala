import org.apache.spark.sql.SparkSession

object Day9PairRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 9 Pair RDD")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // ==========================================
    // 1. CREATE PAIR RDD
    // ==========================================

    println("\n===== PAIR RDD =====")

    val sales = sc.parallelize(Seq(
      ("Laptop", 50000),
      ("Phone", 30000),
      ("Laptop", 45000),
      ("Tablet", 20000),
      ("Phone", 25000),
      ("Tablet", 15000)
    ), 2)

    println("Original sales:")
    sales.collect().foreach(println)


    // ==========================================
    // 2. reduceByKey
    // ==========================================

    println("\n===== REDUCE BY KEY =====")

    val totalSales = sales.reduceByKey((a, b) => a + b)

    println("Total sales by product:")
    totalSales.collect().foreach(println)


    // ==========================================
    // 3. mapValues
    // ==========================================

    println("\n===== MAP VALUES =====")

    val increasedSales = totalSales.mapValues(value => value + 1000)

    println("Sales after adding 1000:")
    increasedSales.collect().foreach(println)


    // ==========================================
    // 4. sortByKey
    // ==========================================

    println("\n===== SORT BY KEY =====")

    val sortedSales = totalSales.sortByKey()

    println("Sales sorted by product:")
    sortedSales.collect().foreach(println)


    // ==========================================
    // 5. groupByKey
    // ==========================================

    println("\n===== GROUP BY KEY =====")

    val groupedSales = sales.groupByKey()

    println("Values grouped by product:")

    groupedSales.collect().foreach {
      case (product, values) =>
        println(product + " -> " + values.mkString(", "))
    }


    // ==========================================
    // 6. reduceByKey VS groupByKey
    // ==========================================

    println("\n===== REDUCE BY KEY VS GROUP BY KEY =====")

    println("reduceByKey:")
    println("- Combines values with the same key.")
    println("- Performs local aggregation before shuffle.")
    println("- Usually more efficient for aggregation.")

    println("\ngroupByKey:")
    println("- Groups all values belonging to the same key.")
    println("- Transfers values across the network.")
    println("- Can use more memory.")


    // ==========================================
    // 7. PAIR RDD CONCEPT
    // ==========================================

    println("\n===== PAIR RDD CONCEPT =====")

    println("A Pair RDD stores data as (key, value).")
    println("Example:")
    println("(Laptop, 50000)")
    println("(Phone, 30000)")

    println("\nCommon Pair RDD operations:")
    println("- reduceByKey")
    println("- groupByKey")
    println("- mapValues")
    println("- sortByKey")


    // ==========================================
    // 8. SUMMARY
    // ==========================================

    println("\n===== SUMMARY =====")

    println("Pair RDDs store data as key-value pairs.")
    println("reduceByKey aggregates values with the same key.")
    println("groupByKey groups values with the same key.")
    println("mapValues modifies only the values.")
    println("sortByKey sorts records according to their keys.")

    spark.stop()
  }
}
