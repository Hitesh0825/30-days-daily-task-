# Day 6 — Word Count

## Objective

The objective of Day 6 is to implement the classic Word Count program using Apache Spark RDDs and understand the use of `flatMap`, `map`, and `reduceByKey`.

The program is also modified to perform case-insensitive word counting, ignore punctuation and empty words, and find the Top 10 most frequent words in application logs.

## Concepts Covered

- Classic Word Count
- `flatMap`
- `map`
- `reduceByKey`
- Case-insensitive counting
- Removing punctuation
- Removing empty words
- Top 10 frequent words
- Spark RDD transformations

## How Word Count Works

The basic Word Count flow is:

`flatMap → map → reduceByKey`

### 1. flatMap

`flatMap` splits each line into individual words.

Example:

`Spark is fast`

becomes:

`Spark, is, fast`

Unlike `map`, `flatMap` can produce multiple output elements from one input element.

### 2. map

`map` converts every word into a key-value pair.

Example:

`Spark`

becomes:

`(Spark, 1)`

The `1` represents one occurrence of the word.

### 3. reduceByKey

`reduceByKey` combines values belonging to the same key.

Example:

`(Spark, 1), (Spark, 1), (Spark, 1)`

becomes:

`(Spark, 3)`

Therefore:

`flatMap → map → reduceByKey`

is the standard Spark RDD Word Count pattern.

## Case-Insensitive Word Count

The program converts every word to lowercase using:

`toLowerCase`

Therefore:

`Spark`, `spark`, and `SPARK`

are counted as the same word.

## Ignoring Punctuation and Empty Words

Punctuation is removed using a regular expression.

Example:

`Spark!`

becomes:

`spark`

Empty words are removed using `filter`.

This prevents empty strings from appearing in the final word count.

## Application Log Scenario

The program uses application log messages such as:

`ERROR Spark failed to start!`

`INFO Application started successfully.`

The logs are processed to find the most frequently occurring words.

The program:

1. Splits log lines into words.
2. Removes punctuation.
3. Converts words to lowercase.
4. Removes empty words.
5. Creates `(word, 1)` pairs.
6. Uses `reduceByKey` to calculate frequencies.
7. Finds the Top 10 most frequent words.

## Transformations Used

| Transformation | Purpose |
|---|---|
| `flatMap` | Splits lines into words |
| `map` | Creates `(word, 1)` pairs |
| `filter` | Removes empty words |
| `reduceByKey` | Combines counts for the same word |

All these operations are transformations and are evaluated lazily until an action is called.

## Technologies

- Scala
- Apache Spark
- Spark RDD
- sbt
- Java 17

## Project Structure

```text
Day6/
├── README.md
├── build.sbt
└── src/
    └── main/
        └── scala/
            └── Day6WordCount.scala
How to Run

Compile the project:

sbt compile

Run the application:

sbt "runMain Day6WordCount"

Key Learnings
flatMap is useful for splitting data into multiple elements.
map transforms each element.
reduceByKey combines values having the same key.
Converting words to lowercase provides case-insensitive counting.
Punctuation and empty words can be removed during preprocessing.
Spark transformations are lazy.
Word Count is a fundamental Spark RDD example.
The same approach can be applied to application log analysis.
Conclusion

Day 6 demonstrates how Spark RDD transformations can be combined to build a practical Word Count application. The project also shows how text preprocessing and reduceByKey can be used to analyze application logs and identify the Top 10 most frequent words.
