Day 11 — Broadcast and Accumulators

📌 Objective

The objective of Day 11 is to understand Broadcast Variables and Accumulators in Apache Spark and apply them in a practical transaction-validation scenario.

This project demonstrates:

Broadcasting a small product reference map

Processing transaction data using an RDD

Validating transactions against broadcast reference data

Counting invalid records using an accumulator

Understanding why normal driver variables should not be used for distributed updates

Combining broadcast data with distributed RDD processing

📚 Concepts Covered
Broadcast Variables

Accumulators

Spark RDDs

Distributed Processing

Small Reference Data

Executor-side Data Access

Driver Variables vs Accumulators

Transaction Validation

Invalid Record Counting

Broadcast + RDD Processing

🔹 Broadcast Variables
A Broadcast Variable is a read-only variable that Spark efficiently makes available to executors.

Broadcast variables are useful when a relatively small dataset is required by many tasks.

Instead of repeatedly sending the same reference data with task operations, Spark can distribute the data as a broadcast variable.

Example
The project creates a small product master table:

val productMaster = Map(
  "P001" -> "Laptop",
  "P002" -> "Phone",
  "P003" -> "Tablet",
  "P004" -> "Monitor"
)
The map is broadcast using:

val broadcastProducts = sc.broadcast(productMaster)
Executors access the broadcast data using:

broadcastProducts.value
🔹 Why Use Broadcast Variables?
Broadcast variables are useful when:

The reference data is relatively small.

Many Spark tasks need the same read-only data.

The same data would otherwise be repeatedly sent to tasks.

Distributed records need to be validated against a small master/reference table.

In this project, the product master table is small and read-only, making it suitable for broadcasting.

🔹 Broadcast Processing Flow
                 DRIVER
                   |
                   |
          Product Master Map
                   |
                   | Broadcast
                   v
              EXECUTORS
                   |
                   |
          Transaction RDD
                   |
                   v
        Validate Product IDs
             /          \
            /            \
        VALID           INVALID
          |                |
          |                |
          v                v
   Product Name      Accumulator +1
🔹 Master Product Table
The application uses the following product reference table:

Product ID	Product
P001	Laptop
P002	Phone
P003	Tablet
P004	Monitor
This table acts as the master data used to validate incoming transactions.

🔹 Transaction Dataset
The application processes these transactions:

T001 -> P001 -> Quantity 2
T002 -> P002 -> Quantity 1
T003 -> P003 -> Quantity 3
T004 -> P999 -> Quantity 2
T005 -> P001 -> Quantity 1
T006 -> P004 -> Quantity 2
T007 -> P888 -> Quantity 1
The valid product IDs are:

P001
P002
P003
P004
Therefore:

P999 -> Invalid
P888 -> Invalid
🔹 Accumulators
An Accumulator is a Spark variable designed for aggregating information from distributed tasks.

Accumulators are commonly useful for counters such as:

Invalid records

Rejected records

Malformed records

Records satisfying a particular condition

The project creates a Long Accumulator:

val badRecords = sc.longAccumulator("Bad Records")
When an invalid transaction is detected:

badRecords.add(1)
The final value is read using:

badRecords.value
🔹 Transaction Validation
Each transaction is checked against the broadcast product master.

The validation flow is:

Transaction
     |
     v
Read Product ID
     |
     v
Check Broadcast Product Map
     |
     +-------------------+
     |                   |
   Found              Not Found
     |                   |
     v                   v
  VALID              INVALID
     |                   |
     |                   +--> Accumulator +1
     |
     v
Product Name
🔹 Validation Results
The successfully executed application produced:

T001 -> VALID | P001 -> Laptop | Quantity: 2
T002 -> VALID | P002 -> Phone | Quantity: 1
T003 -> VALID | P003 -> Tablet | Quantity: 3
T004 -> INVALID | Unknown Product ID: P999
T005 -> VALID | P001 -> Laptop | Quantity: 1
T006 -> VALID | P004 -> Monitor | Quantity: 2
T007 -> INVALID | Unknown Product ID: P888
The final number of invalid records was:

Total bad records: 2
🔹 Driver Variables vs Accumulators
A normal driver-side variable should not be used as a distributed counter.

Spark executors run tasks independently. Changes to ordinary driver variables inside distributed task execution are not a reliable mechanism for collecting updates back to the driver.

For distributed counters, Spark provides accumulators.

Normal Driver Variable
Driver Variable
      |
      X
Executor 1
Executor 2
Executor 3
A normal driver variable is not designed for distributed updates.

Accumulator
             Accumulator
              /    |    \
             /     |     \
        Task 1   Task 2   Task 3
           +1       +1       +1
              \     |     /
               \    |    /
                  Driver
Accumulators provide the appropriate mechanism for task-side counter updates.

🔹 Why Accumulators Are Used
In this project, the accumulator counts invalid transaction records.

Whenever a transaction contains an unknown product ID:

badRecords.add(1)
After the RDD action completes, the driver reads:

badRecords.value
The final result is:

Total bad records: 2
🔹 Combining Broadcast Data with RDD Processing
The main workflow combines a broadcast variable with RDD processing:

Small Product Master
        |
        v
     Broadcast
        |
        v
   Spark Executors
        |
        v
   Transaction RDD
        |
        v
Validate Product IDs
        |
   +----+----+
   |         |
 VALID     INVALID
   |         |
   |         +----> Accumulator
   |
   v
Product Name
This allows distributed transaction data to be validated using a small reference dataset.

🔹 Practical Scenario
Transaction Validation Against a Master Table
Imagine a company receives a large number of transaction records.

Each transaction contains:

Transaction ID
Product ID
Quantity
The company also has a small product master table:

Product ID -> Product Name
The requirement is to:

Validate every transaction.

Check whether the product exists.

Return valid transactions with product names.

Identify invalid transactions.

Count invalid transactions.

Spark can broadcast the small product master table and process the transaction RDD across executors.

🔹 Spark Components Used
Component	Purpose
SparkSession	Creates the Spark application
SparkContext	Provides access to Spark functionality
RDD	Stores transaction data
broadcast()	Distributes the small product master
broadcast.value	Accesses broadcast data
longAccumulator()	Creates a distributed counter
add()	Increments the accumulator
value	Reads the accumulator result
map()	Performs transaction validation
collect()	Retrieves final results
🔹 Important Code Sections
Creating the Broadcast Variable
val broadcastProducts = sc.broadcast(productMaster)
Accessing Broadcast Data
val productMap = broadcastProducts.value
Creating the Accumulator
val badRecords = sc.longAccumulator("Bad Records")
Updating the Accumulator
badRecords.add(1)
Reading the Final Value
badRecords.value
🔹 Complete Processing Flow
                Spark Driver
                     |
        +------------+------------+
        |                         |
        v                         v
 Product Master              Transaction RDD
        |                         |
        | Broadcast               |
        v                         |
    Executors <-------------------+
        |
        v
 Validate Product ID
        |
   +----+----+
   |         |
 VALID     INVALID
   |         |
   v         v
Product    Accumulator
 Name        +1
   |         |
   +----+----+
        |
        v
   Final Results
🛠️ Technologies Used
Technology	Version / Purpose
Scala	2.12.19
Apache Spark	3.5.3
Spark Core	RDD processing
Spark SQL	Spark application dependency
sbt	2.0.8
Java	17.0.20.1
Ubuntu	WSL2 environment
📁 Project Structure
Day11/
├── README.md
├── build.sbt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── Day11BroadcastAccumulator.scala
▶️ How to Run
Step 1 — Navigate to the project
cd ~/30-days-daily-task-/Day11
Step 2 — Run the application
sbt run
The application runs Spark in local mode:

.master("local[*]")
📊 Expected Output
The application displays the master product table:

===== MASTER PRODUCT TABLE =====

P001 -> Laptop
P002 -> Phone
P003 -> Tablet
P004 -> Monitor
Then:

===== BROADCAST CREATED =====

Product master data has been broadcast to executors.
The transaction records are then validated.

The final accumulator result is:

===== ACCUMULATOR RESULT =====

Total bad records: 2

🧠 Key Learnings
After completing Day 11, the following concepts were demonstrated:

Broadcast variables efficiently distribute small read-only reference data.

Executors can access broadcast data using .value.

Accumulators are useful for distributed counters.

Normal driver variables should not be used as distributed counters.

RDDs can be processed using broadcast reference data.

Invalid records can be counted using an accumulator.

Small master/reference tables are good candidates for broadcasting.

Broadcast variables and accumulators solve different distributed-processing problems.

Transaction validation can combine both concepts in a practical Spark workflow.

📌 Day 11 Summary
The project implemented a complete transaction-validation workflow using Spark RDDs.

Product Master
      |
      v
Broadcast Variable
      |
      v
Transaction RDD
      |
      v
Product Validation
      |
   +--+--+
   |     |
 VALID INVALID
         |
         v
   Accumulator
         |
         v
Bad Record Count
The application successfully validated transactions against a small broadcast product master table and used an accumulator to count invalid records.

🏁 Conclusion
Day 11 provided practical experience with two important Spark distributed-programming features: Broadcast Variables and Accumulators.

The project demonstrated how a small product master table can be broadcast to executors and reused during distributed RDD processing. It also demonstrated how an accumulator can count invalid transaction records across distributed tasks.

The transaction-validation scenario showed how both features can work together:

Broadcast → Share Reference Data
Accumulator → Count Distributed Events
RDD → Process Transactions
Understanding these concepts is important for building efficient Spark applications that work with distributed datasets and small reference tables.

