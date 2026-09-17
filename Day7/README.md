# Day 7 — Immutability, Lineage and Fault Tolerance

## Objective

The objective of Day 7 is to understand RDD immutability, lineage, and fault tolerance in Apache Spark.

The program creates a multi-step RDD transformation chain, demonstrates how Spark maintains lineage information, explains why RDDs are immutable, and describes how Spark can recover lost partitions through lineage when an executor failure occurs.

## Concepts Covered

- RDD Immutability
- RDD Lineage
- Fault Tolerance
- RDD Transformations
- Narrow Transformations
- Lazy Evaluation
- RDD Partitions
- Lost Partition Recovery
- Executor Failure
- `toDebugString`
- Spark Actions

## Multi-Step RDD Transformation Chain

The program creates an RDD containing numbers from 1 to 10 and applies multiple transformations.

The transformation flow is:

`Original RDD → filter → map → filter → collect`

Example:

The original RDD contains:

`1, 2, 3, 4, 5, 6, 7, 8, 9, 10`

First, `filter` selects only the even numbers:

`2, 4, 6, 8, 10`

Next, `map` doubles each value:

`4, 8, 12, 16, 20`

Finally, another `filter` selects values greater than 5:

`8, 12, 16, 20`

The final result is produced when the `collect` action is executed.

## RDD Immutability

RDDs are immutable, which means that an existing RDD cannot be directly modified.

For example:

`numbers`

is the original RDD.

When `filter` is applied:

`numbers → evenNumbers`

Spark creates a new RDD instead of modifying the original RDD.

Similarly:

`evenNumbers → doubledNumbers`

creates another RDD.

Therefore, the original RDD remains unchanged.

The transformation flow is:

`numbers → evenNumbers → doubledNumbers → finalRDD`

RDD immutability allows Spark to maintain a reliable transformation history and supports lineage-based fault tolerance.

## RDD Lineage

RDD lineage represents the sequence of transformations used to create an RDD.

For this project, the lineage can be represented as:

```text
numbers
   |
filter(x % 2 == 0)
   |
map(x * 2)
   |
filter(x > 5)
   |
collect()

##  Relationship Between Immutability, Lineage and Fault Tolerance

#  These three concepts are closely connected.

Immutability

RDDs cannot be directly modified.

Lineage

Spark records the transformations used to create new RDDs.

Fault Tolerance

If required data is lost, Spark can use the lineage to recompute it.

The relationship can be represented as:

Immutable RDDs
      ↓
Transformation History
      ↓
RDD Lineage
      ↓
Fault Recovery
      ↓
Recompute Required Data

Therefore, RDD immutability and lineage together provide an important foundation for Spark's fault-tolerance model.

Execution Flow

##  The complete execution flow of this project is:

Create SparkSession
       ↓
Create SparkContext
       ↓
Create Original RDD
       ↓
Filter Even Numbers
       ↓
Double the Numbers
       ↓
Filter Values Greater Than 5
       ↓
Display Final Result
       ↓
Display RDD Lineage
       ↓
Demonstrate Immutability
       ↓
Explain Fault Tolerance
       ↓
Conceptually Simulate Executor Loss
       ↓
Stop SparkSession

##   Expected Result

For the input numbers:

1, 2, 3, 4, 5, 6, 7, 8, 9, 10

The first filter produces:

2, 4, 6, 8, 10

The map transformation produces:

4, 8, 12, 16, 20

The final filter produces:

8, 12, 16, 20

##  The program also displays:

Original RDD values
Final RDD values
RDD lineage
Immutability explanation
Fault-tolerance explanation
Lost partition scenario
Conceptual executor-loss scenario
Summary of the concepts
Technologies
Scala
Apache Spark
Spark RDD
SparkSession
SparkContext
sbt
Java 17

## Project Structure
Day7/
├── README.md
├── build.sbt
└── src/
    └── main/
        └── scala/
            └── Day7LineageFaultTolerance.scala

## How to Run

Navigate to the Day 7 project directory:

cd ~/30-days-daily-task-/Day7

# Compile the project:

sbt compile

# Run the Spark application:

sbt run

# The application runs Spark in local mode using:

local[*]

This allows Spark to use the available local CPU cores for execution.

Key Learnings

## After completing Day 7, the following concepts are understood:

RDDs are immutable.
Transformations create new RDDs.
Spark maintains lineage information for RDDs.
Lineage records the transformation history of an RDD.
Transformations are evaluated lazily.
filter and map are narrow transformations.
collect is an action.
RDDs are divided into partitions for parallel processing.
Lost partitions can be recomputed using lineage.
Executor failures can result in lost partitions that Spark can recover through recomputation.
toDebugString can be used to inspect RDD lineage information.
Immutability and lineage are important parts of Spark's fault-tolerance model.

## Conclusion

Day 7 demonstrates the relationship between RDD immutability, lineage, partitions, and fault tolerance in Apache Spark.

The project shows how multiple transformations create new RDDs while preserving the original RDD. It also demonstrates how Spark maintains lineage information and can use that information to recompute required data when a partition is lost.

Understanding these concepts provides a strong foundation for learning Spark's execution model, distributed processing, and fault-tolerance mechanisms.
