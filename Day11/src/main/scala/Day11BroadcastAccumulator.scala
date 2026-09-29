import org.apache.spark.sql.SparkSession

object Day11BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 11 Broadcast and Accumulators")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // =========================================================
    // 1. SMALL MASTER PRODUCT TABLE
    // =========================================================

    println("\n===== MASTER PRODUCT TABLE =====")

    val productMaster = Map(
      "P001" -> "Laptop",
      "P002" -> "Phone",
      "P003" -> "Tablet",
      "P004" -> "Monitor"
    )

    productMaster.foreach {
      case (id, name) =>
        println(id + " -> " + name)
    }


    // =========================================================
    // 2. BROADCAST THE MASTER DATA
    // =========================================================

    val broadcastProducts = sc.broadcast(productMaster)

    println("\n===== BROADCAST CREATED =====")
    println("Product master data has been broadcast to executors.")


    // =========================================================
    // 3. TRANSACTION DATA
    // =========================================================

    println("\n===== TRANSACTIONS =====")

    val transactions = sc.parallelize(Seq(
      ("T001", "P001", 2),
      ("T002", "P002", 1),
      ("T003", "P003", 3),
      ("T004", "P999", 2),
      ("T005", "P001", 1),
      ("T006", "P004", 2),
      ("T007", "P888", 1)
    ))

    transactions.collect().foreach {
      case (transactionId, productId, quantity) =>
        println(
          transactionId +
            " -> Product: " +
            productId +
            ", Quantity: " +
            quantity
        )
    }


    // =========================================================
    // 4. ACCUMULATOR FOR BAD RECORDS
    // =========================================================

    val badRecords = sc.longAccumulator("Bad Records")

    println("\n===== TRANSACTION VALIDATION =====")

    val validatedTransactions = transactions.map {

      case (transactionId, productId, quantity) =>

        val productMap = broadcastProducts.value

        if (productMap.contains(productId)) {

          val productName = productMap(productId)

          transactionId +
            " -> VALID | " +
            productId +
            " -> " +
            productName +
            " | Quantity: " +
            quantity

        } else {

          badRecords.add(1)

          transactionId +
            " -> INVALID | Unknown Product ID: " +
            productId
        }
    }


    // =========================================================
    // 5. ACTION
    // =========================================================

    validatedTransactions.collect().foreach(println)


    // =========================================================
    // 6. BAD RECORD COUNT
    // =========================================================

    println("\n===== ACCUMULATOR RESULT =====")

    println(
      "Total bad records: " +
        badRecords.value
    )


    // =========================================================
    // 7. NORMAL DRIVER VARIABLE EXPLANATION
    // =========================================================

    println("\n===== DRIVER VARIABLE VS ACCUMULATOR =====")

    println(
      "Normal driver variables should not be used " +
        "for distributed updates."
    )

    println(
      "Executors run tasks independently, so updates " +
        "to a normal driver variable are not reliably " +
        "returned to the driver."
    )

    println(
      "Accumulators are designed for distributed " +
        "counters and aggregations."
    )


    // =========================================================
    // 8. BROADCAST + RDD PROCESSING FLOW
    // =========================================================

    println("\n===== BROADCAST + RDD PROCESSING =====")

    println("Driver")
    println("  |")
    println("  | Broadcast product master")
    println("  v")
    println("Executors")
    println("  |")
    println("  | Validate transactions")
    println("  v")
    println("Valid / Invalid Records")


    // =========================================================
    // 9. FINAL SUMMARY
    // =========================================================

    println("\n===== FINAL SUMMARY =====")

    println(
      "Broadcast variable: Efficiently shares small " +
        "read-only data with executors."
    )

    println(
      "Accumulator: Allows tasks to safely contribute " +
        "to a counter."
    )

    println(
      "Scenario: Transactions were validated against " +
        "a small product master table."
    )

    println(
      "Bad records were counted using an accumulator."
    )


    // =========================================================
    // 10. STOP SPARK
    // =========================================================

    spark.stop()
  }
}
