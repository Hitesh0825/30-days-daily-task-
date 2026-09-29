# Day 12 — Cache and Persist in Apache Spark

## 1. Project Overview

Day 12 focuses on understanding **RDD caching and persistence in Apache Spark**.

In Spark applications, the same RDD may be reused by multiple actions. If the RDD is not cached or persisted, Spark may need to recompute its transformation lineage every time the RDD is used.

This project demonstrates how `cache()` and `persist()` can be used to store an RDD after computation and reuse it efficiently across multiple reports.

The project uses a **transaction-processing scenario** in which a cleaned transaction dataset is reused to generate three different reports:

1. Transaction Count
2. Total Sales
3. Product-wise Sales

The project also demonstrates different storage levels and explains when caching improves performance and when it can negatively affect application performance.

---

# 2. Objectives

The main objectives of this project are:

- Understand RDD caching in Apache Spark.
- Use `cache()` to reuse an RDD across multiple actions.
- Understand the purpose of `persist()`.
- Compare `cache()` and `persist()`.
- Experiment with different Spark storage levels.
- Understand the concept of lazy evaluation and RDD computation.
- Understand when caching improves performance.
- Understand when caching can hurt performance.
- Use `unpersist()` to remove cached data.
- Apply caching to a practical transaction-processing scenario.

---

# 3. Problem Statement

Consider a transaction-processing application that receives a large number of transaction records.

Before generating reports, the raw transaction data must be cleaned.

The cleaned dataset is then required by multiple reports.

Without caching, Spark may repeatedly execute the transformations required to create the cleaned dataset.

This project solves the problem by caching the cleaned RDD so that it can be reused by multiple actions.

The processing flow is:

```text
Raw Transaction Data
        |
        v
   Data Cleaning
        |
        v
Cleaned Transaction RDD
        |
        |--------> Report 1: Transaction Count
        |
        |--------> Report 2: Total Sales
        |
        |--------> Report 3: Product Sales
4. Technologies Used
Technology	Purpose
Scala	Application development
Apache Spark	Distributed data processing
Spark RDD	Distributed data abstraction
SBT	Build and dependency management
Java 17	Runtime environment
WSL2 Ubuntu	Development environment
5. Project Structure
Day12/
│
├── README.md
├── build.sbt
│
├── project/
│   └── build.properties
│
└── src/
    └── main/
        └── scala/
            └── Day12CachePersist.scala
6. Scenario
The application processes transaction records containing:

Transaction ID
Product
Amount
Status
Example transaction:

(T001, Laptop, 50000.0, VALID)
Where:

T001 is the transaction ID.

Laptop is the product.

50000.0 is the transaction amount.

VALID represents the transaction status.

The application first removes invalid transactions and then caches the cleaned dataset.

7. Sample Transaction Data
The project uses sample transaction records such as:

(T001, Laptop, 50000.0, VALID)
(T002, Phone, 30000.0, VALID)
(T003, Tablet, 20000.0, VALID)
(T004, Laptop, -5000.0, INVALID)
(T005, Phone, 25000.0, VALID)
(T006, Monitor, 15000.0, VALID)
(T007, Tablet, 18000.0, VALID)
(T008, Phone, 0.0, INVALID)
8. Data Cleaning
The application removes invalid transactions using the following conditions:

Amount > 0
Status == VALID
The cleaning operation is implemented using the Spark RDD filter() transformation.

Conceptually:

val cleanedTransactions = transactions
  .filter {
    case (_, _, amount, status) =>
      amount > 0 && status == "VALID"
  }
Only valid transactions are retained for further processing.

9. Understanding Cache
Spark provides the cache() method to mark an RDD for caching.

Example:

val cleanedTransactions = transactions
  .filter {
    case (_, _, amount, status) =>
      amount > 0 && status == "VALID"
  }
  .cache()
Calling cache() does not immediately execute the transformation.

Spark follows lazy evaluation.

The RDD is materialized when an action such as count(), collect(), or sum() is executed.

Once computed, Spark can keep the RDD according to its caching storage level and reuse it for subsequent actions.

10. Why Cache the Cleaned Dataset?
The cleaned transaction RDD is used by multiple reports.

Without caching, Spark may need to recompute the transformation lineage when different actions are executed.

For example:

Raw Data
   |
Filter
   |
Report 1
Then:

Raw Data
   |
Filter
   |
Report 2
And again:

Raw Data
   |
Filter
   |
Report 3
With caching:

Raw Data
   |
Filter
   |
Cached RDD
   |
   +------------> Report 1
   |
   +------------> Report 2
   |
   +------------> Report 3
The cached RDD can be reused by subsequent actions.

11. Report 1 — Transaction Count
The first report calculates the number of valid transactions.

The application uses the count() action:

val transactionCount = cleanedTransactions.count()
Example output:

===== REPORT 1 - TRANSACTION COUNT =====
Total valid transactions: 6
This action also causes the cleaned RDD to be evaluated.

12. Report 2 — Total Sales
The second report calculates the total sales amount.

The transaction amounts are extracted using map() and then aggregated using sum().

Example:

val totalSales = cleanedTransactions
  .map {
    case (_, _, amount, _) => amount
  }
  .sum()
Example output:

===== REPORT 2 - TOTAL SALES =====
Total sales: ₹158000.0
Because the cleaned RDD is cached, it can be reused for this report.

13. Report 3 — Product-wise Sales
The third report calculates the total sales for every product.

The cleaned transaction RDD is grouped according to product.

Example:

val productSales = cleanedTransactions
  .groupBy {
    case (_, product, _, _) => product
  }
The values are then aggregated to calculate the total sales for each product.

Example output:

===== REPORT 3 - PRODUCT SALES =====

Laptop -> ₹50000.0
Monitor -> ₹15000.0
Phone -> ₹55000.0
Tablet -> ₹38000.0
14. Cache vs Persist
Spark provides both cache() and persist() for storing RDDs.

14.1 cache()
The cache() method stores an RDD using Spark's default storage level.

Example:

cleanedTransactions.cache()
It is convenient when the default storage behavior is sufficient.

14.2 persist()
The persist() method allows the application to explicitly specify the storage level.

Example:

cleanedTransactions.persist(StorageLevel.MEMORY_ONLY)
Another example:

cleanedTransactions.persist(StorageLevel.MEMORY_AND_DISK)
Therefore:

cache()
    |
    +---- Uses the default storage level


persist()
    |
    +---- Allows explicit storage-level selection
15. Storage Levels
Spark supports different storage levels for persisted RDDs.

This project demonstrates:

MEMORY_ONLY

MEMORY_AND_DISK

16. MEMORY_ONLY
The MEMORY_ONLY storage level keeps the RDD in memory.

Example:

memoryOnlyTransactions.persist(
  StorageLevel.MEMORY_ONLY
)
Advantages:

Fast access.

No disk read required when data is available in memory.

Limitation:

Requires sufficient executor memory.

If the dataset cannot fit in memory, partitions may need to be recomputed.

17. MEMORY_AND_DISK
The MEMORY_AND_DISK storage level stores partitions in memory when possible.

If some partitions cannot fit in memory, Spark can store them on disk.

Example:

memoryAndDiskTransactions.persist(
  StorageLevel.MEMORY_AND_DISK
)
This can be useful when the dataset is larger than the available memory.

The trade-off is that accessing disk-stored partitions is slower than accessing memory-resident partitions.

18. Checking Storage Level
The application prints the storage level using:

cleanedTransactions.getStorageLevel
This allows the application to demonstrate how Spark is configured to store the RDD.

Example:

Storage level: StorageLevel(memory, 1 replicas)
19. Lazy Evaluation
Spark transformations are evaluated lazily.

For example:

val cleanedTransactions = transactions
  .filter(...)
  .cache()
The filter() operation does not immediately process every record.

Spark waits until an action is executed.

Examples of actions include:

count()
collect()
sum()
When the first action is executed, Spark computes the required partitions.

Because the RDD has been marked for caching, Spark can retain the computed data for subsequent operations.

20. Cache and Multiple Actions
The major benefit of caching in this project comes from using the same RDD multiple times.

The cleaned transaction dataset is used by:

Action 1
   |
count()
Action 2
   |
sum()
Action 3
   |
groupBy()
The overall design is:

                    Cleaned RDD
                         |
                       cache
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
       count()          sum()       groupBy()
          |              |              |
          v              v              v
       Report 1       Report 2       Report 3
21. When Caching Improves Performance
Caching is useful when:

The same dataset is used multiple times.

The transformation that creates the dataset is expensive.

The dataset is reused across multiple actions.

The dataset fits reasonably well in available memory.

The cost of recomputation is higher than the cost of storage.

For example:

Expensive Transformation
          |
          v
      Cached RDD
       /   |   \
      /    |    \
Report 1 Report 2 Report 3
The expensive transformation does not need to be repeatedly performed when the cached data is available.

22. When Caching Can Hurt Performance
Caching is not automatically beneficial.

It can hurt performance in certain situations.

22.1 Dataset Used Only Once
If an RDD is used only once, caching may add unnecessary storage overhead.

22.2 Dataset Is Very Large
A very large dataset may consume significant executor memory.

22.3 Insufficient Memory
If there is not enough memory available, Spark may need to evict cached partitions or use disk depending on the storage level.

22.4 Too Many Cached Datasets
Caching too many RDDs can create memory pressure.

This may result in:

Increased garbage collection

Eviction of cached data

Increased disk usage

Lower application performance

22.5 Cheap Transformations
If the transformation is inexpensive, recomputing the data may sometimes be cheaper than storing it.

23. Unpersist
Once cached or persisted data is no longer required, it should be removed.

Spark provides the unpersist() method.

Example:

cleanedTransactions.unpersist()
The project also removes the explicitly persisted RDDs:

memoryOnlyTransactions.unpersist()

memoryAndDiskTransactions.unpersist()
This helps release storage resources.

24. Complete Processing Architecture
                     Transaction Data
                            |
                            v
                    Data Cleaning
                            |
                            v
                 Cleaned Transaction RDD
                            |
                         cache()
                            |
             +--------------+--------------+
             |              |              |
             v              v              v
       Transaction       Total Sales    Product Sales
          Count             Report          Report
             |              |              |
             +--------------+--------------+
                            |
                            v
                       unpersist()
25. Execution Flow
The application follows these steps:

1. Create SparkSession
2. Create transaction RDD
3. Display raw transaction data
4. Remove invalid transactions
5. Cache the cleaned RDD
6. Generate transaction-count report
7. Generate total-sales report
8. Generate product-sales report
9. Demonstrate MEMORY_ONLY
10. Demonstrate MEMORY_AND_DISK
11. Explain cache vs persist
12. Explain caching limitations
13. Unpersist temporary datasets
14. Stop Spark

 How to Run the Project

Navigate to the Day 12 directory:

cd ~/30-days-daily-task-/Day12
Clean the previous build:

sbt clean
Run the application:

sbt run
27. Expected Output
The application produces output similar to:

===== RAW TRANSACTION DATA =====

===== CLEANED TRANSACTIONS =====

===== CACHE =====
Cleaned transaction dataset has been cached.

===== REPORT 1 - TRANSACTION COUNT =====
Total valid transactions: 6

===== REPORT 2 - TOTAL SALES =====
Total sales: ₹158000.0

===== REPORT 3 - PRODUCT SALES =====

Laptop -> ₹50000.0
Monitor -> ₹15000.0
Phone -> ₹55000.0
Tablet -> ₹38000.0

===== PERSIST - MEMORY_ONLY =====

===== PERSIST - MEMORY_AND_DISK =====

===== CACHE VS PERSIST =====

===== WHEN CACHING CAN HURT PERFORMANCE =====

===== UNPERSIST =====

===== FINAL SUMMARY =====
The exact formatting and Spark log messages may vary depending on the local Spark environment.

## Important Spark Concepts
RDD
An RDD is a distributed collection of objects that can be processed in parallel across a Spark cluster.

Transformation
A transformation creates a new RDD from an existing RDD.

Examples used in this project include:

filter()
map()
groupBy()
Transformations are lazily evaluated.

Action
An action triggers Spark computation.

Examples used in this project include:

count()
collect()
sum()
Cache
cache() marks an RDD to be stored for reuse.

Persist
persist() allows a specific storage level to be selected.

Unpersist
unpersist() removes an RDD from Spark's storage memory.

RDD caching

RDD persistence

Lazy evaluation

Storage levels

cache()

persist()

MEMORY_ONLY

MEMORY_AND_DISK

unpersist()

Reusing RDDs across multiple actions

Performance considerations of caching

31. Real-World Application
Caching is commonly useful in data-processing systems where the same cleaned or transformed dataset is required by multiple downstream operations.

For example:

                  Raw Transactions
                         |
                         v
                  Data Cleaning
                         |
                         v
                 Cached Dataset
                         |
        +----------------+----------------+
        |                |                |
        v                v                v
   Sales Report     Product Report   Customer Report
Instead of repeating the cleaning process for every report, the application can reuse the cached dataset.

This pattern can be useful in:

Transaction analytics

Financial reporting

Customer analytics

Log processing

ETL pipelines

Machine learning workflows

Interactive analytics

32. Conclusion
Day 12 demonstrates the importance of caching and persistence in Apache Spark.

The project uses a cleaned transaction RDD and reuses it across multiple reports. The cache() method is used to mark the RDD for reuse, while persist() demonstrates how different storage levels can be selected according to application requirements.

The project also demonstrates MEMORY_ONLY and MEMORY_AND_DISK storage levels and explains the trade-offs involved in choosing them.

Most importantly, caching should not be treated as a default optimization for every Spark application. It is most useful when a dataset is expensive to compute and is reused multiple times.

The key principle learned from this project is:

Cache data when recomputation is expensive and the dataset will be reused. Avoid unnecessary caching because it consumes cluster storage resources.

33. Author
Hitesh Sharma

B.Tech — Data Science & Artificial Intelligence

GitHub:

https://github.com/Hitesh0825
34. Learning Progress
Day 12
  |
  +-- RDD Cache
  |
  +-- RDD Persist
  |
  +-- Storage Levels
  |
  +-- Lazy Evaluation
  |
  +-- Multiple Actions
  |
  +-- Performance Optimization
  |
  +-- Unpersist
Day 12 completed — Cache and Persist in Apache Spark.


### Put it into Nano

```bash
cd ~/30-days-daily-task-/Day12
nano README.md
Delete the existing contents, paste the entire README above, then:

Ctrl + O
Enter
Ctrl + X
Then verify:

cat README.md
After that:

cd ~/30-days-daily-task-
git status
Send me that git status output and we'll do the Day 12 Git add → commit → rebase → push cleanly.


give me in a document 

Done. I created the complete professional Day 12 README in Markdown document form, ready to copy directly into Day12/README.md.

Download Day12 README.md 


Day12_README.md
Document




Ad

Library
/
Day12_README.md



Day 12 — Cache and Persist in Apache Spark
📌 Overview
Day 12 focuses on RDD caching and persistence in Apache Spark.

The objective is to understand how Spark can keep a frequently reused RDD in memory or other storage locations so that repeated actions do not need to recompute the same transformation pipeline.

This project uses a cleaned transaction dataset that is reused across multiple reports.

🎯 Learning Objectives
Understand how cache() works in Spark.

Understand how persist() provides configurable storage levels.

Compare cache() and persist().

Experiment with different storage levels.

Understand when caching improves performance.

Understand when caching can hurt performance.

Reuse a cleaned RDD across multiple reports.

Release persisted data using unpersist().

🏗️ Scenario
A transaction-processing system first cleans raw transaction data. The cleaned dataset is then reused to generate three reports:

Transaction Count Report

Revenue Report

Product Sales Report

Instead of recomputing the cleaning transformations for every report, Spark can persist the cleaned RDD and reuse it.

Processing Flow
Raw Transactions
       |
       v
   Clean Data
       |
       v
 Cache / Persist
       |
       +--------------------+
       |                    |
       v                    v
Report 1                 Report 2
Transaction Count       Revenue Report
       |
       v
   Report 3
Product Sales
🧰 Technologies Used
Apache Spark 3.5.3

Scala 2.12.19

sbt 2.0.8

Java 17

Ubuntu / WSL2

📂 Project Structure
Day12/
├── README.md
├── build.sbt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── Day12CachePersist.scala
⚙️ Core Spark Concepts
1. RDD
An RDD (Resilient Distributed Dataset) is Spark's fundamental distributed data structure.

RDDs are:

Distributed across the cluster.

Immutable.

Fault-tolerant.

Lazily evaluated.

2. Lazy Evaluation
Spark transformations are not executed immediately.

For example:

val cleanedTransactions = transactions
  .filter(...)
  .map(...)
The transformations are recorded, but Spark executes them only when an action such as count() or collect() is called.

Without persistence, Spark may recompute the transformation lineage whenever the RDD is required by another action.

💾 Cache
The cache() method tells Spark to persist an RDD using its default storage level.

cleanedTransactions.cache()
The cached RDD is materialized when an action is executed:

cleanedTransactions.count()
After that, Spark can reuse the persisted data for later actions when the cached partitions are available.

When to Use Cache
Caching is useful when:

The same RDD is used multiple times.

The RDD is expensive to compute.

The RDD fits reasonably well in available memory.

Multiple reports reuse the same dataset.

💽 Persist
persist() allows the application to explicitly select a storage level.

cleanedTransactions.persist(StorageLevel.MEMORY_ONLY)
Another example:

cleanedTransactions.persist(StorageLevel.MEMORY_AND_DISK)
This provides more control than cache().

🔄 Cache vs Persist
Feature	cache()	persist()
Purpose	Persist an RDD	Persist an RDD
Storage level	Uses default storage level	Storage level can be selected
Control	Less control	More control
Usage	Simple repeated reuse	Specific storage requirements
Example	rdd.cache()	rdd.persist(StorageLevel.MEMORY_ONLY)
Key Point
cache() is a convenient way to persist an RDD using Spark's default storage level.

📦 Storage Levels
MEMORY_ONLY
Stores the RDD in memory.

rdd.persist(StorageLevel.MEMORY_ONLY)
If the data does not fit in memory, partitions that cannot be stored may need to be recomputed.

MEMORY_AND_DISK
Stores data in memory when possible. If it does not fit, remaining partitions are stored on disk.

rdd.persist(StorageLevel.MEMORY_AND_DISK)
This is useful when the dataset is too large to fit completely in memory.

DISK_ONLY
Stores the persisted RDD on disk.

rdd.persist(StorageLevel.DISK_ONLY)
This reduces memory usage but disk access is slower than memory access.

📊 Three Reports Using the Same Cached Dataset
The cleaned transaction RDD is reused to generate three reports.

Report 1 — Transaction Count
Counts the number of cleaned transactions.

cleanedTransactions.count()
Report 2 — Revenue
Calculates total revenue from the cleaned transactions.

cleanedTransactions
  .map(...)
  .sum()
Report 3 — Product Sales
Groups transactions by product and calculates product-level sales.

This demonstrates that the same persisted RDD can be reused for another action without rebuilding the complete cleaning pipeline.

🚀 Execution Flow
1. Create SparkSession
        |
        v
2. Create raw transaction RDD
        |
        v
3. Clean transaction data
        |
        v
4. Cache / Persist cleaned RDD
        |
        v
5. Run Report 1
        |
        v
6. Run Report 2
        |
        v
7. Run Report 3
        |
        v
8. Compare storage approaches
        |
        v
9. Unpersist RDD
        |
        v
10. Stop Spark
⚡ Why Caching Improves Performance
Suppose the cleaned dataset is created using several transformations:

Raw Data
   |
filter
   |
map
   |
parse
   |
clean
   |
Report
Without persistence, another report may trigger the same transformations again.

With caching:

Raw Data
   |
filter
   |
map
   |
clean
   |
Cached RDD
   |
   +----> Report 1
   |
   +----> Report 2
   |
   +----> Report 3
The expensive transformation pipeline can be avoided for subsequent actions when the cached partitions are available.

⚠️ When Caching Can Hurt Performance
Caching is not always beneficial.

1. Dataset Is Used Only Once
If an RDD is used by only one action, caching may provide little or no benefit.

2. Dataset Is Too Large
If the dataset is larger than available memory, Spark may evict cached partitions or spill data to disk.

3. Memory Pressure
Caching consumes executor memory that could otherwise be used for computation.

4. Cheap Computations
If recomputing an RDD is inexpensive, caching may add unnecessary overhead.

5. Too Many Cached RDDs
Persisting many datasets at the same time can cause memory pressure and eviction.

Best Practice
Cache data when it is expensive to compute and reused multiple times.

🧹 Unpersist
Once the cached RDD is no longer required, remove it from memory or storage.

cleanedTransactions.unpersist()
This releases resources used by the persisted RDD.

🔍 Cache Lifecycle
Create RDD
   |
   v
cache() / persist()
   |
   v
First Action
   |
   v
RDD Is Materialized
   |
   v
Subsequent Actions
   |
   v
Reuse Persisted Partitions
   |
   v
unpersist()
🧪 Running the Project
Navigate to the Day 12 directory:

cd ~/30-days-daily-task-/Day12
Run the Spark application:

sbt run
For a clean build:

sbt clean
sbt run
📋 Expected Output
The application demonstrates sections similar to:

===== RAW TRANSACTIONS =====

===== CLEANED TRANSACTIONS =====

===== CACHE / PERSIST =====

===== REPORT 1 - TRANSACTION COUNT =====

===== REPORT 2 - TOTAL REVENUE =====

===== REPORT 3 - PRODUCT SALES =====

===== STORAGE LEVEL COMPARISON =====

===== CACHE PERFORMANCE CONCEPT =====

===== UNPERSIST =====

===== FINAL SUMMARY =====
The exact transaction values and report results depend on the data defined in Day12CachePersist.scala.

🧠 Important Differences
Transformation
A transformation creates a new RDD.

Examples:

map()
filter()
flatMap()
Action
An action triggers execution.

Examples:

count()
collect()
reduce()
Cache
Marks an RDD for persistence using the default storage level.

rdd.cache()
Persist
Allows an explicit storage level.

rdd.persist(StorageLevel.MEMORY_ONLY)
Unpersist
Removes persisted data.

rdd.unpersist()
🏆 Key Takeaways
Spark uses lazy evaluation for transformations.

An RDD can be persisted when it is reused.

cache() provides convenient default persistence.

persist() provides control over the storage level.

MEMORY_ONLY keeps data in memory when possible.

MEMORY_AND_DISK uses memory first and disk when necessary.

DISK_ONLY stores persisted data on disk.

Caching is most useful for expensive RDDs reused by multiple actions.

Caching unnecessary data can waste memory and reduce performance.

unpersist() should be used when persisted data is no longer needed.

📚 Conclusion

The project covers:

RDD caching

RDD persistence

Storage levels

Multiple actions

Performance considerations

Resource management with unpersist()

This provides a foundation for understanding how Spark applications optimize repeated computations and manage distributed data efficiently.

👨‍💻 Author
Hitesh Sharma

B.Tech — Data Science & Artificial Intelligence

GitHub: Hitesh0825

⭐ Day 12 Completed
Topic: Cache and Persist
Framework: Apache Spark
Language: Scala
Focus: RDD Persistence, Storage Levels & Performance Optimization

