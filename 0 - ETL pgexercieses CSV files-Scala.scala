// Databricks notebook source
// MAGIC %md
// MAGIC # Learning Objectives
// MAGIC
// MAGIC In this notebook, you will 
// MAGIC - learn the concept of ETL
// MAGIC - write ETL jobs for CSV files from `pgexercises` https://pgexercises.com/gettingstarted.html

// COMMAND ----------

// MAGIC %md
// MAGIC # What's ETL or ELT?
// MAGIC
// MAGIC ETL stands for Extract, Transform, Load. In the context of Spark, ETL refers to the process of extracting data from various sources, transforming it into a desired format or structure, and loading it into a target system, such as a data warehouse or a data lake.
// MAGIC
// MAGIC Here's a breakdown of each step in the ETL process:
// MAGIC
// MAGIC ## Extract
// MAGIC This step involves extracting data from multiple sources, such as databases, files (CSV, JSON, Parquet), APIs, or streaming data sources. Spark provides connectors and APIs to read data from a wide range of sources, allowing you to extract data in parallel and efficiently handle large datasets.
// MAGIC
// MAGIC ## Transform
// MAGIC In the transform step, the extracted data is processed and transformed according to specific business logic or requirements. This may involve cleaning the data, applying calculations or aggregations, performing data enrichment, filtering, joining datasets, or any other data manipulation operations. Spark provides a powerful set of transformation functions and SQL capabilities to perform these operations efficiently in a distributed and scalable manner.
// MAGIC
// MAGIC ## Load
// MAGIC Once the data has been transformed, it is loaded into a target system, such as a data warehouse, a data lake, or another storage system. Spark allows you to write the transformed data to various output formats and storage systems, including databases, distributed file systems (like Hadoop Distributed File System or Amazon S3), or columnar formats like Delta Lake or Apache Parquet. The data can be partitioned, sorted, or structured to optimize querying and analysis.
// MAGIC
// MAGIC Spark's distributed computing capabilities, scalability, and rich ecosystem of libraries make it a popular choice for ETL workflows. It can handle large-scale data processing, perform complex transformations, and efficiently load data into different target systems.
// MAGIC
// MAGIC By leveraging Spark for ETL, organizations can extract data from diverse sources, apply transformations to ensure data quality and consistency, and load the transformed data into a central repository for further analysis, reporting, or machine learning tasks.

// COMMAND ----------

// MAGIC %md
// MAGIC # Enable DBFS UI
// MAGIC
// MAGIC - Setting -> Admin Console -> search for dbfs
// MAGIC
// MAGIC <img src="https://raw.githubusercontent.com/jarviscanada/jarvis_data_eng_demo/feature/data/spark/notebook/spark_fundamentals/img/entable_dbfs.jpg" width="700">
// MAGIC
// MAGIC - Refresh the page and view DBFS files from UI
// MAGIC
// MAGIC <img src="https://raw.githubusercontent.com/jarviscanada/jarvis_data_eng_demo/feature/data/spark/notebook/spark_fundamentals/img/dbfs%20ui.png" width="700">

// COMMAND ----------

// MAGIC %md
// MAGIC ## Import `pgexercises` CSV files
// MAGIC
// MAGIC - The pgexercises CSV data files can be found [here](https://github.com/jarviscanada/jarvis_data_eng_demo/tree/feature/data/spark/data/pgexercises).
// MAGIC - The pgexercises schema can be found [here](https://pgexercises.com/gettingstarted.html) (for reference purposes).
// MAGIC - Upload the `bookings.csv`, `facilities.csv`, and `members.csv` files using Databricks UI (see screenshot)
// MAGIC - You can view the imported files from the DBFS UI.
// MAGIC
// MAGIC ![Upload Files](https://raw.githubusercontent.com/jarviscanada/jarvis_data_eng_demo/feature/data/spark/notebook/spark_fundamentals/img/upload%20file.png)

// COMMAND ----------

// MAGIC %md
// MAGIC # Interview Questions
// MAGIC
// MAGIC While completing the rest of the practice, try to answer the following questions:
// MAGIC
// MAGIC ## Concepts
// MAGIC - What is ETL? (Hint: Explain each step)
// MAGIC
// MAGIC ## Databricks
// MAGIC - What is Databricks?
// MAGIC - What is a Notebook?
// MAGIC - What is DBFS?
// MAGIC - What is a cluster? 
// MAGIC - Is Databricks a data lake or a data warehouse?
// MAGIC
// MAGIC ## Managed Table
// MAGIC - What is a managed table in Databricks?
// MAGIC - Can you explain how to create a managed table in Databricks?
// MAGIC - Can you compare a managed table with an RDBMS table? (Hint: Schema on read vs schema on write)
// MAGIC - What is the Hive metastore and how does it relate to managed tables in Databricks?
// MAGIC - How does a managed table differ from an unmanaged (external) table in Databricks? (Hint: Consider what happens to the data when the table is deleted)
// MAGIC - How can you define a schema for a managed table?
// MAGIC
// MAGIC ## Spark
// MAGIC `df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_location)`
// MAGIC - What does the option("inferSchema", "true") do? 
// MAGIC - What does the option("header", "true") do?
// MAGIC - How can you write data to a managed table?
// MAGIC - How can you read data from a managed table into a DataFrame?

// COMMAND ----------

// MAGIC %md
// MAGIC # ETL `bookings.csv` file
// MAGIC
// MAGIC - **Extract**: Load data from CSV file into a DF
// MAGIC - **Transformation**: no transformation needed as we want to load data as it
// MAGIC - **Load**: Save the DF into a managed table (or Hive table); 
// MAGIC
// MAGIC # Managed Table
// MAGIC This is an important interview topic. Some people may refer to managed tables as Hive tables.
// MAGIC
// MAGIC https://docs.databricks.com/data-governance/unity-catalog/create-tables.html

// COMMAND ----------

// MAGIC %md
// MAGIC ###Booking Table

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC DROP TABLE IF EXISTS booking_Scala;
// MAGIC DROP TABLE IF EXISTS members_Scala;
// MAGIC DROP TABLE IF EXISTS facilities_Scala;

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}

// Define the path to your CSV file
val csvFilePath = "/FileStore/tables/bookings.csv"

// Read the CSV file into a DataFrame
val csvData: DataFrame = spark.read
  .format("csv")
  .option("header", "true") // If the CSV has a header row
  .load(csvFilePath)

// Define the name of your managed table
val tableName = "booking_Scala"

// Create the managed table using the DataFrame
csvData.write
  .format("delta") // You can use "delta" format for managed tables
  .mode("overwrite")
  .saveAsTable(tableName)


// COMMAND ----------

// MAGIC %sql
// MAGIC select * from booking_Scala limit 10;

// COMMAND ----------

// MAGIC %md
// MAGIC # Complete ETL Jobs
// MAGIC
// MAGIC - Complete ETL for `facilities.csv` and `members.csv`
// MAGIC - Tips
// MAGIC   - The Databricks community version will terminate the cluster after a few hours of inactivity. As a result, all managed tables will be deleted. You will need to rerun this notebook to perform the ETL on all files for the other exercises.
// MAGIC   - DBFS data will not be deleted when a custer become inactive/deleted

// COMMAND ----------

// MAGIC %md
// MAGIC ###Facilities Table

// COMMAND ----------

import org.apache.spark.sql.DataFrame

// Define the path to your CSV file
val csvFilePath = "/FileStore/tables/facilities.csv"

// Read the CSV file into a DataFrame
val csvData: DataFrame = spark.read
  .format("csv")
  .option("header", "true") // If the CSV has a header row
  .load(csvFilePath)

// Define the name of your managed table
val tableName = "facilities_Scala"

// Create the managed table using the DataFrame
csvData.write
  .format("delta") // You can use "delta" format for managed tables
  .mode("overwrite")
  .saveAsTable(tableName)


// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from facilities_Scala limit 10;

// COMMAND ----------

// MAGIC %md
// MAGIC ###Members Table

// COMMAND ----------

import org.apache.spark.sql.DataFrame

// Define the path to your CSV file
val csvFilePath = "/FileStore/tables/members.csv"

// Read the CSV file into a DataFrame
val csvData: DataFrame = spark.read.format("csv").option("header", "true").load(csvFilePath)

// Define the name of your managed table
//val tableName = "members_Scala"

// Create the managed table using the DataFrame
csvData.write.format("delta").mode("overwrite").saveAsTable("members_Scala")


// COMMAND ----------

import org.apache.spark.sql.DataFrame

// Define the path to your CSV file
val csvFilePath = "/FileStore/tables/members.csv"

// Read the CSV file into a DataFrame
val csvData: DataFrame = spark.read
  .format("csv")
  .option("header", "true") // If the CSV has a header row
  .load(csvFilePath)

// Define the name of your managed table
val tableName = "members_Scala"

// Create the managed table using the DataFrame
csvData.write
  .format("delta") // You can use "delta" format for managed tables
  .mode("overwrite")
  .saveAsTable(tableName)


// COMMAND ----------

// MAGIC %sql
// MAGIC select * from members_Scala limit 10;

// COMMAND ----------

// MAGIC %md
// MAGIC # Save your work to Git
// MAGIC
// MAGIC - Export the notebook to IPYTHON format, `notebook top menu bar -> File -> Export -> iphython`
// MAGIC - Upload to your Git repository, `your_repo/spark/notebooks/`
// MAGIC - Github can render ipython notebook https://github.com/josephcslater/JupyterExamples/blob/master/Calc_Review.ipynb
