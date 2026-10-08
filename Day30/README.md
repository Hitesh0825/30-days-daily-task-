# Day 30 — Final Capstone and Interview Practice

## Domain
E-Commerce Analytics

## Required Concepts Covered
- Batch processing
- Streaming concepts
- RDD
- Pair RDD
- DataFrame
- Spark SQL
- Broadcast
- Accumulator
- Cache/Persist
- Partition tuning
- Joins
- Aggregations
- Window functions
- UDF
- DAG
- Lineage
- Stages
- Executors
- YARN

## Architecture
```text
Raw Transactions
      |
      v
Validation / Cleaning
      |
      v
Broadcast Product Master
      |
      v
Enrichment + Join
      |
      v
Aggregation
      |
      v
Window Analytics
      |
      v
UDF Risk Classification
      |
      v
Spark SQL Report
      |
      v
Partitioned Parquet
```

## 20 Interview Questions
1. What is Spark?
2. What is an RDD?
3. What is a DataFrame?
4. What is a Dataset?
5. What is a transformation?
6. What is an action?
7. What is lazy evaluation?
8. What is lineage?
9. What is a DAG?
10. What is a stage?
11. What is a shuffle?
12. What is broadcast?
13. What is an accumulator?
14. What is cache?
15. What is persist?
16. What is a window function?
17. Why avoid unnecessary UDFs?
18. What is the driver?
19. What is an executor?
20. What is YARN?
