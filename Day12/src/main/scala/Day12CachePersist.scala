import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 12 Cache and Persist")
      .master("local[*]")
      .config(
        "spark.serializer",
        "org.apache.spark.serializer.JavaSerializer"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // RAW TRANSACTION DATA
    // ------------------------------------------------------------

    val transactions = spark.sparkContext.parallelize(
      Seq(
        ("T001", "Laptop", 50000.0, "VALID"),
        ("T002", "Phone", 30000.0, "VALID"),
        ("T003", "Tablet", 20000.0, "VALID"),
        ("T004", "Laptop", -5000.0, "INVALID"),
        ("T005", "Phone", 25000.0, "VALID"),
        ("T006", "Monitor", 15000.0, "VALID"),
        ("T007", "Tablet", 18000.0, "VALID"),
        ("T008", "Phone", 0.0, "INVALID")
      )
    )

    println("\n===== RAW TRANSACTION DATA =====")

    transactions.collect().foreach {
      case (id, product, amount, status) =>
        println(s"($id,$product,$amount,$status)")
    }

    // ------------------------------------------------------------
    // CLEAN TRANSACTIONS
    // ------------------------------------------------------------

    val cleanedTransactions = transactions
      .filter {
        case (_, _, amount, status) =>
          amount > 0 && status == "VALID"
      }
      .cache()

    println("\n===== CLEANED TRANSACTIONS =====")

    cleanedTransactions.collect().foreach {
      case (id, product, amount, status) =>
        println(s"($id,$product,$amount,$status)")
    }

    // ------------------------------------------------------------
    // CACHE
    // ------------------------------------------------------------

    println("\n===== CACHE =====")
    println("Cleaned transaction dataset has been cached.")
    println(
      s"Storage level: ${cleanedTransactions.getStorageLevel}"
    )

    // ------------------------------------------------------------
    // REPORT 1 - TRANSACTION COUNT
    // ------------------------------------------------------------

    println("\n===== REPORT 1 - TRANSACTION COUNT =====")

    val transactionCount = cleanedTransactions.count()

    println(s"Total valid transactions: $transactionCount")

    // ------------------------------------------------------------
    // REPORT 2 - TOTAL SALES
    // ------------------------------------------------------------

    println("\n===== REPORT 2 - TOTAL SALES =====")

    val totalSales = cleanedTransactions
      .map {
        case (_, _, amount, _) => amount
      }
      .sum()

    println(s"Total sales: ₹$totalSales")

    // ------------------------------------------------------------
    // REPORT 3 - PRODUCT SALES
    // ------------------------------------------------------------

    println("\n===== REPORT 3 - PRODUCT SALES =====")

    val productSales = cleanedTransactions
      .groupBy {
        case (_, product, _, _) => product
      }
      .map {
        case (product, records) =>
          val total = records.map {
            case (_, _, amount, _) => amount
          }.sum

          (product, total)
      }
      .collect()
      .sortBy(_._1)

    productSales.foreach {
      case (product, total) =>
        println(s"$product -> ₹$total")
    }

    // ------------------------------------------------------------
    // PERSIST - MEMORY ONLY
    // ------------------------------------------------------------

    println("\n===== PERSIST - MEMORY_ONLY =====")

    val memoryOnlyTransactions = transactions
      .filter {
        case (_, _, amount, status) =>
          amount > 0 && status == "VALID"
      }
      .persist(StorageLevel.MEMORY_ONLY)

    println(
      s"Storage level: ${memoryOnlyTransactions.getStorageLevel}"
    )

    println(
      s"Records stored: ${memoryOnlyTransactions.count()}"
    )

    // ------------------------------------------------------------
    // PERSIST - MEMORY AND DISK
    // ------------------------------------------------------------

    println("\n===== PERSIST - MEMORY_AND_DISK =====")

    val memoryAndDiskTransactions = transactions
      .filter {
        case (_, _, amount, status) =>
          amount > 0 && status == "VALID"
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(
      s"Storage level: ${memoryAndDiskTransactions.getStorageLevel}"
    )

    println(
      s"Records stored: ${memoryAndDiskTransactions.count()}"
    )

    // ------------------------------------------------------------
    // CACHE VS PERSIST
    // ------------------------------------------------------------

    println("\n===== CACHE VS PERSIST =====")

    println("cache(): Uses the default MEMORY_ONLY storage level.")
    println(
      "persist(): Allows selecting a specific storage level."
    )
    println(
      "Examples: MEMORY_ONLY and MEMORY_AND_DISK."
    )

    // ------------------------------------------------------------
    // WHEN CACHING CAN HURT PERFORMANCE
    // ------------------------------------------------------------

    println("\n===== WHEN CACHING CAN HURT PERFORMANCE =====")

    println("Caching can hurt performance when:")
    println("- The dataset is used only once.")
    println("- The dataset is very large and does not fit in memory.")
    println("- Too many datasets are cached at the same time.")
    println("- Memory pressure causes eviction or garbage collection.")
    println("- Recomputing the dataset is cheaper than storing it.")

    // ------------------------------------------------------------
    // UNPERSIST
    // ------------------------------------------------------------

    println("\n===== UNPERSIST =====")

    memoryOnlyTransactions.unpersist()
    memoryAndDiskTransactions.unpersist()

    println("Temporary persisted RDDs have been removed from storage.")

    // ------------------------------------------------------------
    // FINAL SUMMARY
    // ------------------------------------------------------------

    println("\n===== FINAL SUMMARY =====")

    println("Cache was used to reuse the cleaned transaction dataset.")
    println("The cached dataset was reused in three reports.")
    println("Report 1 calculated the number of valid transactions.")
    println("Report 2 calculated total sales.")
    println("Report 3 calculated sales by product.")
    println("persist() was demonstrated with MEMORY_ONLY.")
    println("persist() was demonstrated with MEMORY_AND_DISK.")
    println("Caching should be used when an RDD is reused multiple times.")
    println("Caching unnecessary or very large datasets can reduce performance.")

    // ------------------------------------------------------------
    // CLEANUP
    // ------------------------------------------------------------

    cleanedTransactions.unpersist()

    spark.stop()
  }
}
