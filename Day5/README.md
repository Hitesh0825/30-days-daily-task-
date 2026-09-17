# Day 5 — Transformations and Actions

## Objective
Practice Apache Spark RDD transformations and actions using Scala and understand lazy evaluation.

## Transformations
- map
- filter
- flatMap
- distinct
- union

Transformations create a new RDD from an existing RDD and are lazily evaluated.

## Actions
- count
- collect
- first
- take
- reduce

Actions trigger Spark computation and return a result.

## Transformations vs Actions

| Transformations | Actions |
|---|---|
| map | count |
| filter | collect |
| flatMap | first |
| distinct | take |
| union | reduce |

## Lazy Evaluation

Spark uses lazy evaluation. Transformations do not execute immediately. Spark builds the execution plan and executes it when an action is called.

Lazy transformations:
- map
- filter
- flatMap
- distinct
- union

Actions that trigger execution:
- count
- collect
- first
- take
- reduce

## Transformation Examples

### map
Transforms every element.

Example:
numbers = 1, 2, 3, 4, 5

Using map(x => x * x):

Result = 1, 4, 9, 16, 25

### filter
Keeps elements that satisfy a condition.

Example:
filter(x => x % 2 == 0)

Result = 2, 4

### flatMap
Transforms and flattens data.

Example:
"Spark is fast"

Result:
Spark, is, fast

### distinct
Removes duplicate elements.

Example:
1, 2, 2, 3, 3, 4

Result:
1, 2, 3, 4

### union
Combines two RDDs.

Example:
RDD1 = 1, 2, 3
RDD2 = 4, 5, 6

Result:
1, 2, 3, 4, 5, 6

Note: union does not automatically remove duplicates.

## Action Examples

### count
Returns the number of elements.

Result for 1, 2, 3, 4, 5:
5

### collect
Returns all elements to the driver.

### first
Returns the first element.

Result:
1

### take
Returns the first N elements.

take(3):
1, 2, 3

### reduce
Combines elements using a function.

Example:
1 + 2 + 3 + 4 + 5 = 15

## Log Analyzer

A simple Spark Log Analyzer was created to count ERROR messages.

Sample logs:

INFO User login successful
ERROR Database connection failed
INFO User viewed dashboard
ERROR File not found
WARNING Disk space is low
ERROR Authentication failed
INFO User logout successful
ERROR Database timeout
INFO Application started

The ERROR messages are filtered using filter().

The number of ERROR messages is calculated using count().

Expected result:

Total ERROR messages: 4

## Technologies Used

- Scala 2.12.19
- Apache Spark 3.5.3
- Spark Core
- Spark SQL
- sbt
- Java 17
- Ubuntu / WSL2

## Project Structure

Day5/
├── README.md
├── build.sbt
└── src/
    └── main/
        └── scala/
            └── Day5Transformations.scala

## How to Run

cd Day5
sbt clean
sbt compile
sbt "runMain Day5Transformations"

## Key Learnings

- Spark RDDs
- Transformations
- Actions
- Lazy evaluation
- map
- filter
- flatMap
- distinct
- union
- count
- collect
- first
- take
- reduce
- Log analysis

## Conclusion

Day 5 provided practical experience with Spark RDD transformations and actions. The task demonstrated lazy evaluation and how actions trigger Spark execution. A Log Analyzer was also implemented to filter and count ERROR messages from application logs.
