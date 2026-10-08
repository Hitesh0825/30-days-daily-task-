# Day 29 — End-to-End E-Commerce

## Pipeline
RAW -> CLEAN -> ENRICH -> JOIN -> AGGREGATE -> WINDOW -> OUTPUT

## Concepts
- RDD / Pair RDD
- DataFrame / Dataset
- Joins
- Windows
- Partitioning
- Persistence
- Shuffle and stages
- Optimization
- UDF only where required

## Architecture
```text
Raw Data
   |
   v
Cleaning
   |
   v
Enrichment / Joins
   |
   v
Aggregation
   |
   v
Window Analytics
   |
   v
Partitioned Output
```
