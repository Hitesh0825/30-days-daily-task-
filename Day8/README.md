
# Day 8 — DAG and Spark Execution

## 📌 Objective

The objective of Day 8 is to understand how Apache Spark creates and executes a **Directed Acyclic Graph (DAG)** from transformations and actions.

This project demonstrates how Spark organizes computations into **jobs, stages, tasks, and partitions**, and explains the difference between **narrow and wide transformations** and the role of **shuffle operations** in Spark execution.

---

## 📚 Concepts Covered

- Spark DAG
- Jobs
- Stages
- Tasks
- Partitions
- Narrow Transformations
- Wide Transformations
- Shuffle
- Shuffle Boundaries
- `reduceByKey`
- `sortByKey`
- RDD Lineage
- Lazy Evaluation
- Stage Prediction

---

## 🔄 Spark DAG

**DAG** stands for **Directed Acyclic Graph**.

Spark creates a DAG based on the transformations and actions defined in a Spark application.

For this project, the conceptual execution flow is:

```text
numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey
   |
mapValues
   |
sortByKey
   |
collect()
Spark analyzes this transformation chain and divides the computation into stages based on shuffle dependencies.

🔗 Transformation Pipeline
The program starts by creating an RDD containing numbers from 1 to 20.

The transformation pipeline is:

numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey
   |
mapValues
   |
sortByKey
   |
collect()
The pipeline contains both narrow and wide transformations.

🔹 Narrow Transformations
A narrow transformation does not require a full shuffle of data between partitions.

The following transformations used in this project are narrow transformations:

filter

map

mapValues

For example:

numbers → filter → map
Each output partition depends on a limited number of input partitions.

Because these operations do not require a shuffle, Spark can generally execute them within the same stage.

Examples
filter
Selects elements that satisfy a condition.

numbers.filter(x => x % 2 == 0)
map
Transforms each element.

evenNumbers.map(x => x * 2)
mapValues
Transforms the values of a Pair RDD while keeping the keys unchanged.

reduced.mapValues(x => x * 10)
🔸 Wide Transformations
A wide transformation requires data to be redistributed between partitions.

The following transformations used in this project are wide transformations:

reduceByKey

sortByKey

These operations can create shuffle dependencies.

For example:

map → reduceByKey
Values with the same key may exist in different partitions. Spark therefore needs to redistribute the data so that related keys can be processed together.

🔀 Shuffle
A shuffle occurs when Spark redistributes data between partitions.

For example, reduceByKey combines values belonging to the same key.

Conceptually:

Partition 0 ──┐
              |
Partition 1 ──┼──> Shuffle ──> New Partitions
              |
Partition 2 ──┘
Shuffle operations are important because they can create boundaries between Spark stages.

🚧 Shuffle Boundaries
A shuffle boundary separates stages in Spark's execution plan.

In this project:

filter → map → Pair RDD
                  |
             reduceByKey
                  |
               SHUFFLE
The reduceByKey operation introduces a shuffle dependency.

Later:

reduceByKey → mapValues
                   |
               sortByKey
                   |
                SHUFFLE
sortByKey also introduces a shuffle dependency.

Therefore, the computation contains multiple stages separated by shuffle boundaries.

🏗️ Stages
A stage is a group of operations that can be executed together without a shuffle dependency between them.

For the main pipeline, the conceptual stage structure is:

Stage 1
numbers → filter → map → map to Pair RDD

        ↓
     SHUFFLE

Stage 2
reduceByKey → mapValues

        ↓
     SHUFFLE

Stage 3
sortByKey
Shuffle dependencies determine the boundaries between these stages.

💼 Jobs
A Spark job is created when an action is executed.

In this project, actions such as:

collect()
trigger Spark computation.

The general execution flow is:

Action
   ↓
Job
   ↓
Stages
   ↓
Tasks
   ↓
Partitions
⚙️ Tasks
A task is the unit of work used to process one partition within a stage.

Conceptually:

Stage
├── Task 0 → Partition 0
├── Task 1 → Partition 1
├── Task 2 → Partition 2
└── Task 3 → Partition 3
Therefore, the number of tasks in a stage is related to the number of partitions that the stage needs to process.

🧩 Partitions
A partition is a logical division of an RDD.

In this project, the input RDD is created using:

sc.parallelize(1 to 20, 4)
Therefore, the input RDD contains 4 partitions.

Conceptually:

RDD
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
Spark can process these partitions in parallel.

🔗 Jobs, Stages, Tasks and Partitions
These four concepts are closely related.

Job
 ↓
Stages
 ↓
Tasks
 ↓
Partitions
Job
A complete computation triggered by an action.

Stage
A group of operations separated from other stages by shuffle dependencies.

Task
A unit of work that processes a partition within a stage.

Partition
A logical portion of an RDD that can be processed independently.

🌐 DAG and RDD Lineage
The program uses:

toDebugString
to display RDD dependency information.

The conceptual DAG for this project is:

numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey
   |
mapValues
   |
sortByKey
   |
collect()
The important shuffle dependencies are:

map to Pair RDD
       |
   reduceByKey
       |
    SHUFFLE
       |
   mapValues
       |
   sortByKey
       |
    SHUFFLE
🔢 Stage Prediction Scenario
Consider the following pipeline:

map → filter → reduceByKey → mapValues → collect
The first two transformations are narrow:

map → filter
Then reduceByKey introduces a shuffle dependency.

The simplified stage structure is:

Stage 1
map → filter

      ↓
   SHUFFLE

Stage 2
reduceByKey → mapValues
Therefore, for this simplified pipeline, the expected number of stages is:

2 stages

The shuffle dependency introduced by reduceByKey separates the two stages.

💰 Example — reduceByKey Pipeline
The project creates key-value pairs using:

(x % 3, x)
For example:

(1, 4)
(2, 8)
(0, 12)
(1, 16)
(2, 20)
The reduceByKey operation combines values belonging to the same key.

For example:

(1, 4)
(1, 16)
becomes:

(1, 20)
Values belonging to the same key may be located in different partitions, so Spark may need to redistribute them during the shuffle.

🔤 sortByKey
The project also uses:

sortByKey()
to sort the reduced key-value pairs by their keys.

Sorting distributed data can require redistribution of data between partitions.

Therefore, sortByKey can introduce another shuffle dependency and stage boundary.

⏳ Lazy Evaluation
Spark uses lazy evaluation for transformations.

When a transformation is defined, Spark does not immediately execute it.

For example:

numbers.filter(x => x % 2 == 0)
does not immediately process all the data.

Instead, Spark records the transformation as part of the execution plan.

The actual computation begins when an action such as:

collect()
is executed.

The general flow is:

Transformations
      ↓
DAG
      ↓
Stages
      ↓
Tasks
      ↓
Execution
Lazy evaluation allows Spark to organize and optimize the required computation before execution.

⚖️ Narrow vs Wide Transformations
Type	Transformations	Shuffle
Narrow	filter, map, mapValues	No full shuffle
Wide	reduceByKey, sortByKey	Requires data redistribution
Narrow Transformation
Does not require a full shuffle.

Output partitions depend on a limited number of input partitions.

Multiple narrow transformations can generally be executed within the same stage.

Wide Transformation
Requires data redistribution between partitions.

Creates a shuffle dependency.

Can create a stage boundary.

🔄 Complete Execution Flow
The complete execution flow of this project is:

Create SparkSession
       ↓
Create SparkContext
       ↓
Create Input RDD
       ↓
Filter Even Numbers
       ↓
Double the Numbers
       ↓
Create Pair RDD
       ↓
reduceByKey
       ↓
Shuffle Dependency
       ↓
mapValues
       ↓
sortByKey
       ↓
Shuffle Dependency
       ↓
collect()
       ↓
Display DAG and Lineage
       ↓
Explain Jobs, Stages, Tasks and Partitions
       ↓
Display Summary
📊 Expected Result
The program processes numbers from:

1 to 20
The even numbers are selected:

2, 4, 6, 8, 10, 12, 14, 16, 18, 20
These values are doubled:

4, 8, 12, 16, 20, 24, 28, 32, 36, 40
The values are then converted into key-value pairs using:

(x % 3, x)
The reduceByKey operation combines values belonging to the same key.

The final result is processed using:

mapValues
and:

sortByKey
The application also displays:

Input RDD

Number of partitions

Narrow transformation results

Pair RDD

reduceByKey result

Final sorted result

DAG / lineage information

Shuffle dependencies

Stage explanation

Jobs, stages, tasks and partitions

Narrow vs wide transformation comparison

Stage prediction scenario

🛠️ Technologies
Scala

Apache Spark

Spark RDD

SparkSession

SparkContext

sbt

Java 17

📁 Project Structure
Day8/
├── README.md
├── build.sbt
└── src/
    └── main/
        └── scala/
            └── Day8DAGExecution.scala
▶️ How to Run
1. Navigate to the Day 8 Directory
cd ~/30-days-daily-task-/Day8
2. Compile the Project
sbt compile
3. Run the Spark Application
sbt run
The application runs Spark in local mode using:

local[*]
This allows Spark to use the available local CPU cores for execution.

🧠 Key Learnings
After completing Day 8, the following concepts are understood:

Spark creates a DAG from transformations and actions.

A Spark job is triggered by an action.

A job is divided into stages.

Shuffle dependencies separate stages.

Tasks process partitions.

Partitions enable parallel processing.

filter and map are narrow transformations.

mapValues is a narrow transformation.

reduceByKey is a wide transformation.

sortByKey can introduce a shuffle dependency.

Wide transformations can create stage boundaries.

Lazy evaluation delays transformation execution until an action is called.

toDebugString can be used to inspect RDD dependency information.

Understanding shuffle is important for understanding Spark performance.

🎤 Viva / Interview Questions
1. What is a DAG in Spark?
DAG stands for Directed Acyclic Graph. Spark uses a DAG to represent the sequence of transformations and dependencies required to perform a computation.

2. What is a Spark Job?
A Spark job is a computation triggered by an action such as collect().

3. What is a Stage?
A stage is a group of operations separated from other stages by shuffle dependencies.

4. What is a Task?
A task is a unit of work that processes one partition within a stage.

5. What is a Partition?
A partition is a logical division of an RDD that allows Spark to process data in parallel.

6. What is a Shuffle?
A shuffle is the redistribution of data between partitions.

7. What is a Shuffle Boundary?
A shuffle boundary separates stages because data needs to be redistributed before the dependent computation can continue.

8. What is the difference between Narrow and Wide Transformations?
Narrow transformations do not require a full shuffle, while wide transformations require data redistribution between partitions.

9. Is reduceByKey a Narrow or Wide Transformation?
reduceByKey is a wide transformation because values belonging to the same key may need to be redistributed between partitions.

10. Why does reduceByKey create a Shuffle?
Values with the same key can exist in different partitions. Spark redistributes the data so that values belonging to the same key can be combined.

11. Why is map a Narrow Transformation?
Each output element depends on a corresponding input element, so map does not require a full redistribution of data.

12. What triggers a Spark Job?
An action such as collect(), count(), or save() triggers execution of the required computation.

13. How are Tasks related to Partitions?
A stage typically creates a task for each partition that it needs to process.

14. How many stages are expected for map → filter → reduceByKey → mapValues → collect?
For this simplified pipeline, the shuffle dependency introduced by reduceByKey results in two conceptual stages:

Stage 1:
map → filter

Stage 2:
reduceByKey → mapValues
15. Does every Transformation create a new Stage?
No. Narrow transformations can generally be executed together within the same stage. Shuffle dependencies create boundaries between stages.

📌 Summary
The main Spark execution concepts covered in Day 8 are:

DAG
 ↓
Job
 ↓
Stages
 ↓
Tasks
 ↓
Partitions
And the transformation categories are:

Narrow Transformations
        ↓
filter / map / mapValues

Wide Transformations
        ↓
reduceByKey / sortByKey
        ↓
Shuffle
        ↓
Stage Boundary
🏁 Conclusion
Day 8 demonstrates how Apache Spark converts a sequence of transformations and actions into a Directed Acyclic Graph (DAG) and executes the computation through jobs, stages, tasks, and partitions.

The project explains the difference between narrow and wide transformations and demonstrates how operations such as reduceByKey and sortByKey can introduce shuffle dependencies.

Understanding DAGs, stages, tasks, partitions, and shuffle operations provides a strong foundation for understanding how Spark executes distributed data-processing applications and how transformation design can affect execution and performance.


### Nano mein paste karne

give me in a documnet form so that i can copy paste in readme file totally 

Bilkul bhai ✅ complete professional Day 8 README.md document ready hai — proper #, ##, ### headings ke saath, directly nano mein copy-paste kar sakte ho.

Download:
📄 Day8_README_Professional.md

Nano mein:

cd ~/30-days-daily-task-/Day8
nano README.md
File ka poora content ek baar mein copy → paste → Ctrl+O → Enter → Ctrl+X. 


Day8_README_Professional.md
Document


Library
/
Day8_README_Professional.md


Day 8 — DAG and Spark Execution
📌 Objective
The objective of Day 8 is to understand how Apache Spark creates and executes a Directed Acyclic Graph (DAG) from transformations and actions.

This project demonstrates how Spark organizes computations into jobs, stages, tasks, and partitions, and explains the difference between narrow and wide transformations and the role of shuffle operations in Spark execution.

📚 Concepts Covered
Spark DAG

Jobs

Stages

Tasks

Partitions

Narrow Transformations

Wide Transformations

Shuffle

Shuffle Boundaries

reduceByKey

sortByKey

RDD Lineage

Lazy Evaluation

Stage Prediction

🔄 Spark DAG
DAG stands for Directed Acyclic Graph.

Spark creates a DAG based on the transformations and actions defined in a Spark application.

For this project, the conceptual execution flow is:

numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey
   |
mapValues
   |
sortByKey
   |
collect()
Spark analyzes this transformation chain and divides the computation into stages based on shuffle dependencies.

🔗 Transformation Pipeline
The program starts by creating an RDD containing numbers from 1 to 20.

The transformation pipeline is:

numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey
   |
mapValues
   |
sortByKey
   |
collect()
The pipeline contains both narrow and wide transformations.

🔹 Narrow Transformations
A narrow transformation does not require a full shuffle of data between partitions.

The following transformations used in this project are narrow transformations:

filter

map

mapValues

For example:

numbers → filter → map
Each output partition depends on a limited number of input partitions.

Because these operations do not require a shuffle, Spark can generally execute them within the same stage.

Examples
filter
Selects elements that satisfy a condition.

numbers.filter(x => x % 2 == 0)
map
Transforms each element.

evenNumbers.map(x => x * 2)
mapValues
Transforms the values of a Pair RDD while keeping the keys unchanged.

reduced.mapValues(x => x * 10)
🔸 Wide Transformations
A wide transformation requires data to be redistributed between partitions.

The following transformations used in this project are wide transformations:

reduceByKey

sortByKey

These operations can create shuffle dependencies.

For example:

map → reduceByKey
Values with the same key may exist in different partitions. Spark therefore needs to redistribute the data so that related keys can be processed together.

🔀 Shuffle
A shuffle occurs when Spark redistributes data between partitions.

For example, reduceByKey combines values belonging to the same key.

Conceptually:

Partition 0 ──┐
              |
Partition 1 ──┼──> Shuffle ──> New Partitions
              |
Partition 2 ──┘
Shuffle operations are important because they can create boundaries between Spark stages.

🚧 Shuffle Boundaries
A shuffle boundary separates stages in Spark's execution plan.

In this project:

filter → map → Pair RDD
                  |
             reduceByKey
                  |
               SHUFFLE
The reduceByKey operation introduces a shuffle dependency.

Later:

reduceByKey → mapValues
                   |
               sortByKey
                   |
                SHUFFLE
sortByKey also introduces a shuffle dependency.

Therefore, the computation contains multiple stages separated by shuffle boundaries.

🏗️ Stages
A stage is a group of operations that can be executed together without a shuffle dependency between them.

For the main pipeline, the conceptual stage structure is:

Stage 1
numbers → filter → map → map to Pair RDD

        ↓
     SHUFFLE

Stage 2
reduceByKey → mapValues

        ↓
     SHUFFLE

Stage 3
sortByKey
Shuffle dependencies determine the boundaries between these stages.

💼 Jobs
A Spark job is created when an action is executed.

In this project, actions such as:

collect()
trigger Spark computation.

The general execution flow is:

Action
   ↓
Job
   ↓
Stages
   ↓
Tasks
   ↓
Partitions
⚙️ Tasks
A task is the unit of work used to process one partition within a stage.

Conceptually:

Stage
├── Task 0 → Partition 0
├── Task 1 → Partition 1
├── Task 2 → Partition 2
└── Task 3 → Partition 3
Therefore, the number of tasks in a stage is related to the number of partitions that the stage needs to process.

🧩 Partitions
A partition is a logical division of an RDD.

In this project, the input RDD is created using:

sc.parallelize(1 to 20, 4)
Therefore, the input RDD contains 4 partitions.

Conceptually:

RDD
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
Spark can process these partitions in parallel.

🔗 Jobs, Stages, Tasks and Partitions
These four concepts are closely related.

Job
 ↓
Stages
 ↓
Tasks
 ↓
Partitions
Job
A complete computation triggered by an action.

Stage
A group of operations separated from other stages by shuffle dependencies.

Task
A unit of work that processes a partition within a stage.

Partition
A logical portion of the distributed dataset.

🌐 DAG and RDD Lineage
The program uses:

toDebugString
to display RDD dependency information.

The conceptual DAG for this project is:

numbers
   |
filter
   |
map
   |
map to Pair RDD
   |
reduceByKey
   |
mapValues
   |
sortByKey
   |
collect()
The important shuffle dependencies are:

map to Pair RDD
       |
   reduceByKey
       |
    SHUFFLE
       |
   mapValues
       |
   sortByKey
       |
    SHUFFLE
🔢 Stage Prediction Scenario
Consider the following pipeline:

map → filter → reduceByKey → mapValues → collect
The first two transformations are narrow:

map → filter
Then reduceByKey introduces a shuffle dependency.

The simplified stage structure is:

Stage 1
map → filter

      ↓
   SHUFFLE

Stage 2
reduceByKey → mapValues
Therefore, for this simplified pipeline, the expected number of stages is:

2 stages

The shuffle dependency introduced by reduceByKey separates the two stages.

💰 Example — reduceByKey Pipeline
The project creates key-value pairs using:

(x % 3, x)
For example:

(1, 4)
(2, 8)
(0, 12)
(1, 16)
(2, 20)
The reduceByKey operation combines values belonging to the same key.

For example:

(1, 4)
(1, 16)
becomes:

(1, 20)
Values belonging to the same key may be located in different partitions, so Spark may need to redistribute them during the shuffle.

🔤 sortByKey
The project also uses:

sortByKey()
to sort the reduced key-value pairs by their keys.

Sorting distributed data can require redistribution of data between partitions.

Therefore, sortByKey can introduce another shuffle dependency and stage boundary.

⏳ Lazy Evaluation
Spark uses lazy evaluation for transformations.

When a transformation is defined, Spark does not immediately execute it.

For example:

numbers.filter(x => x % 2 == 0)
does not immediately process all the data.

Instead, Spark records the transformation as part of the execution plan.

The actual computation begins when an action such as:

collect()
is executed.

The general flow is:

Transformations
      ↓
DAG
      ↓
Stages
      ↓
Tasks
      ↓
Execution
Lazy evaluation allows Spark to organize and optimize the required computation before execution.

⚖️ Narrow vs Wide Transformations
Type	Transformations	Shuffle
Narrow	filter, map, mapValues	No full shuffle
Wide	reduceByKey, sortByKey	Requires data redistribution
Narrow Transformation
Does not require a full shuffle.

Output partitions depend on a limited number of input partitions.

Multiple narrow transformations can generally be executed within the same stage.

Wide Transformation
Requires data redistribution between partitions.

Creates a shuffle dependency.

Can create a stage boundary.

🔄 Complete Execution Flow
The complete execution flow of this project is:

Create SparkSession
       ↓
Create SparkContext
       ↓
Create Input RDD
       ↓
Filter Even Numbers
       ↓
Double the Numbers
       ↓
Create Pair RDD
       ↓
reduceByKey
       ↓
Shuffle Dependency
       ↓
mapValues
       ↓
sortByKey
       ↓
Shuffle Dependency
       ↓
collect()
       ↓
Display DAG and Lineage
       ↓
Explain Jobs, Stages, Tasks and Partitions
       ↓
Display Summary
📊 Expected Result
The program processes numbers from:

1 to 20
The even numbers are selected:

2, 4, 6, 8, 10, 12, 14, 16, 18, 20
These values are doubled:

4, 8, 12, 16, 20, 24, 28, 32, 36, 40
The values are then converted into key-value pairs using:

(x % 3, x)
The reduceByKey operation combines values belonging to the same key.

The final result is processed using:

mapValues
and:

sortByKey
The application also displays:

Input RDD

Number of partitions

Narrow transformation results

Pair RDD

reduceByKey result

Final sorted result

DAG / lineage information

Shuffle dependencies

Stage explanation

Jobs, stages, tasks and partitions

Narrow vs wide transformation comparison

Stage prediction scenario

🛠️ Technologies
Scala

Apache Spark

Spark RDD

SparkSession

SparkContext

sbt

Java 17

📁 Project Structure
Day8/
├── README.md
├── build.sbt
└── src/
    └── main/
        └── scala/
            └── Day8DAGExecution.scala
▶️ How to Run
1. Navigate to the Day 8 Directory
cd ~/30-days-daily-task-/Day8
2. Compile the Project
sbt compile
3. Run the Spark Application
sbt run
The application runs Spark in local mode using:

local[*]
This allows Spark to use the available local CPU cores for execution.

🧠 Key Learnings
After completing Day 8, the following concepts are understood:

Spark creates a DAG from transformations and actions.

A Spark job is triggered by an action.

A job is divided into stages.

Shuffle dependencies separate stages.

Tasks process partitions.

Partitions enable parallel processing.

filter and map are narrow transformations.

mapValues is a narrow transformation.

reduceByKey is a wide transformation.

sortByKey can introduce a shuffle dependency.

Wide transformations can create stage boundaries.

Lazy evaluation delays transformation execution until an action is called.

toDebugString can be used to inspect RDD dependency information.

Understanding shuffle is important for understanding Spark performance.

🎤 Viva / Interview Questions
1. What is a DAG in Spark?
DAG stands for Directed Acyclic Graph. Spark uses a DAG to represent the sequence of transformations and dependencies required to perform a computation.

2. What is a Spark Job?
A Spark job is a computation triggered by an action such as collect().

3. What is a Stage?
A stage is a group of operations separated from other stages by shuffle dependencies.

4. What is a Task?
A task is a unit of work that processes one partition within a stage.

5. What is a Partition?
A partition is a logical division of an RDD that allows Spark to process data in parallel.

6. What is a Shuffle?
A shuffle is the redistribution of data between partitions.

7. What is a Shuffle Boundary?
A shuffle boundary separates stages because data needs to be redistributed before the dependent computation can continue.

8. What is the difference between Narrow and Wide Transformations?
Narrow transformations do not require a full shuffle, while wide transformations require data redistribution between partitions.

9. Is reduceByKey a Narrow or Wide Transformation?
reduceByKey is a wide transformation because values belonging to the same key may need to be redistributed between partitions.

10. Why does reduceByKey create a Shuffle?
Values with the same key can exist in different partitions. Spark redistributes the data so that values belonging to the same key can be combined.

11. Why is map a Narrow Transformation?
Each output element depends on a corresponding input element, so map does not require a full redistribution of data.

12. What triggers a Spark Job?
An action such as collect(), count(), or save() triggers execution of the required computation.

13. How are Tasks related to Partitions?
A stage typically creates a task for each partition that it needs to process.

14. How many stages are expected for map → filter → reduceByKey → mapValues → collect?
For this simplified pipeline, the shuffle dependency introduced by reduceByKey results in two conceptual stages:

Stage 1:
map → filter

Stage 2:
reduceByKey → mapValues
15. Does every Transformation create a new Stage?
No. Narrow transformations can generally be executed together within the same stage. Shuffle dependencies create boundaries between stages.

📌 Summary
The main Spark execution concepts covered in Day 8 are:

DAG
 ↓
Job
 ↓
Stages
 ↓
Tasks
 ↓
Partitions
The transformation categories are:

Narrow Transformations
        ↓
filter / map / mapValues

Wide Transformations
        ↓
reduceByKey / sortByKey
        ↓
Shuffle
        ↓
Stage Boundary
🏁 Conclusion
Day 8 demonstrates how Apache Spark converts a sequence of transformations and actions into a Directed Acyclic Graph (DAG) and executes the computation through jobs, stages, tasks, and partitions.

The project explains the difference between narrow and wide transformations and demonstrates how operations such as reduceByKey and sortByKey can introduce shuffle dependencies.

Understanding DAGs, stages, tasks, partitions, and shuffle operations provides a strong foundation for understanding how Spark executes distributed data-processing applications and how transformation design can affect execution and performance.

