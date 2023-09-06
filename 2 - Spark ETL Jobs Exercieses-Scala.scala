// Databricks notebook source
// MAGIC %md
// MAGIC # Learning Objectives
// MAGIC
// MAGIC In this notebook, you will craft sophisticated ETL jobs that interface with a variety of common data sources, such as 
// MAGIC - REST APIs (HTTP endpoints)
// MAGIC - RDBMS
// MAGIC - Hive tables (managed tables)
// MAGIC - Various file formats (csv, json, parquet, etc.)

// COMMAND ----------

// MAGIC %md
// MAGIC d
// MAGIC
// MAGIC # Interview Questions
// MAGIC
// MAGIC As you progress through the practice, attempt to answer the following questions:
// MAGIC
// MAGIC ## Columnar File
// MAGIC - What is a columnar file format and what advantages does it offer?
// MAGIC - Why is Parquet frequently used with Spark and how does it function?
// MAGIC - How do you read/write data from/to a Parquet file using a DataFrame?
// MAGIC
// MAGIC ## Partitions
// MAGIC - How do you save data to a file system by partitions? (Hint: Provide the code)
// MAGIC - How and why can partitions reduce query execution time? (Hint: Give an example)
// MAGIC
// MAGIC ## JDBC and RDBMS
// MAGIC - How do you load data from an RDBMS into Spark? (Hint: Discuss the steps and JDBC)
// MAGIC
// MAGIC ## REST API and HTTP Requests
// MAGIC - How can Spark be used to fetch data from a REST API? (Hint: Discuss making API requests)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ETL Job One: Parquet file
// MAGIC ### Extract
// MAGIC Extract data from the managed tables (e.g. `bookings_csv`, `members_csv`, and `facilities_csv`)
// MAGIC
// MAGIC ### Transform
// MAGIC Data transformation requirements https://pgexercises.com/questions/aggregates/fachoursbymonth.html
// MAGIC
// MAGIC ### Load
// MAGIC Load data into a parquet file
// MAGIC
// MAGIC ### What is Parquet? 
// MAGIC
// MAGIC Columnar files are an important technique for optimizing Spark queries. Additionally, they are often tested in interviews.
// MAGIC - https://www.youtube.com/watch?v=KLFadWdomyI
// MAGIC - https://www.databricks.com/glossary/what-is-parquet

// COMMAND ----------

//Extract data from the managed tables.

val bk = spark.sql("select * from booking_scala")
bk.show(5)

val mem= spark.sql("SELECT* FROM members_scala")
mem.show(5)

val fac= spark.sql("select * from facilities_scala")
fac.show(5)

// COMMAND ----------

//Transform data as requested into dataframe.
//Produce a list of the total number of slots booked per facility in the month of September 2012. Produce an output table consisting of facility id and slots, sorted by the number of slots.
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.IntegerType

val septbk= bk.filter(
    (bk("starttime") >= "2012-09-01") && (bk("starttime") < "2012-10-01")).groupBy("facid").agg(sum("slots").alias("Sum of Slots"))
septbk.show()

// COMMAND ----------

//Load operation
//Writing dataframe into Parquet file.

septbk.write.parquet("/FileStore/tables/slots_Scala.parquet")


// COMMAND ----------

// MAGIC %python
// MAGIC #Reading dataframe from Parquet file.
// MAGIC spark.read.parquet("/FileStore/tables/slots_Scala.parquet").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## ETL Job Two: Partitions
// MAGIC
// MAGIC ### Extract
// MAGIC Extract data from the managed tables (e.g. `bookings_csv`, `members_csv`, and `facilities_csv`)
// MAGIC
// MAGIC ### Transform
// MAGIC Transform the data https://pgexercises.com/questions/joins/threejoin.html
// MAGIC
// MAGIC ### Load
// MAGIC Partition the result data by facility column and then save to `threejoin_delta` managed table. Additionally, they are often tested in interviews.
// MAGIC
// MAGIC hint: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html
// MAGIC
// MAGIC What are paritions? 
// MAGIC
// MAGIC Partitions are an important technique to optimize Spark queries
// MAGIC - https://www.youtube.com/watch?v=hvF7tY2-L3U&t=268s

// COMMAND ----------

// Transform data as per request

//How can you produce a list of all members who have used a tennis court? Include in your output the name of the court, and the name of the member formatted as a single column. Ensure no duplicate data, and order by the member name followed by the facility name.

import org.apache.spark.sql.functions._

val li= List("Tennis Court 1","Tennis Court 2")

val data = mem.join(bk, mem("memid") === bk("memid"), "inner").join(fac, bk("facid") === fac("facid"), "inner")

val data1= data.select( concat(mem("firstname"), lit(" "),mem("surname")).alias("Name"),fac("name").alias("facility") )
.filter((fac("name").isin(li: _*)))

val d= data1.distinct().orderBy("Name","facility")
d.show()


// COMMAND ----------

//Loading data 
//Partition the result data by facility column and then save to threejoin_delta managed table.

d.write.partitionBy("facility").mode("overwrite").format("parquet").save("/FileStore/tables/threejoin_delta_scala.parquet")



// COMMAND ----------

//Reading specific parquet partition.
//Reading data for facility = tennis Court 1.
spark.read.parquet("/FileStore/tables/threejoin_delta_scala.parquet/facility=Tennis Court 1").show()

// COMMAND ----------

//Reading specific parquet partition.
//Reading data for facility = tennis Court 2.
spark.read.parquet("/FileStore/tables/threejoin_delta_scala.parquet/facility=Tennis Court 2").orderBy("name").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## ETL Job Three: HTTP Requests
// MAGIC
// MAGIC ### Extract
// MAGIC Extract daily stock price data price from the following companies, Google, Apple, Microsoft, and Tesla. 
// MAGIC
// MAGIC Data Source
// MAGIC - API: https://rapidapi.com/alphavantage/api/alpha-vantage
// MAGIC - Endpoint: GET `TIME_SERIES_DAILY`
// MAGIC
// MAGIC Sample HTTP request
// MAGIC
// MAGIC ```
// MAGIC curl --request GET \
// MAGIC 	--url 'https://alpha-vantage.p.rapidapi.com/query?function=TIME_SERIES_DAILY&symbol=TSLA&outputsize=compact&datatype=json' \
// MAGIC 	--header 'X-RapidAPI-Host: alpha-vantage.p.rapidapi.com' \
// MAGIC 	--header 'X-RapidAPI-Key: [YOUR_KEY]'
// MAGIC
// MAGIC ```
// MAGIC
// MAGIC Sample Python HTTP request
// MAGIC
// MAGIC ```
// MAGIC import requests
// MAGIC
// MAGIC url = "https://alpha-vantage.p.rapidapi.com/query"
// MAGIC
// MAGIC querystring = {
// MAGIC     "function":"TIME_SERIES_DAILY",
// MAGIC     "symbol":"IBM",
// MAGIC     "datatype":"json",
// MAGIC     "outputsize":"compact"
// MAGIC }
// MAGIC
// MAGIC headers = {
// MAGIC     "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com",
// MAGIC     "X-RapidAPI-Key": "[YOUR_KEY]"
// MAGIC }
// MAGIC
// MAGIC response = requests.get(url, headers=headers, params=querystring)
// MAGIC
// MAGIC data = response.json()
// MAGIC
// MAGIC # Now 'data' contains the daily time series data for "IBM"
// MAGIC ```
// MAGIC
// MAGIC ### Transform
// MAGIC Find **weekly** max closing price for each company.
// MAGIC
// MAGIC hints: 
// MAGIC   - Use a `for-loop` to get stock data for each company
// MAGIC   - Use the spark `union` operation to concat all data into one DF
// MAGIC   - create a new `week` column from the data column
// MAGIC   - use `group by` to calcualte max closing price
// MAGIC
// MAGIC ### Load
// MAGIC - Partition `DF` by company
// MAGIC - Load the DF in to a managed table called, `max_closing_price_weekly`

// COMMAND ----------

// MAGIC %python
// MAGIC # Write your solution here
// MAGIC import requests
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC # Set up your Alpha Vantage API key and base URL
// MAGIC api_key = "4a3c8ab6f6mshf33c91629f0ecd6p194a71jsn0810a5a1a065"
// MAGIC base_url = "https://alpha-vantage.p.rapidapi.com/query"
// MAGIC
// MAGIC # List of companies (symbols) you want to fetch data for
// MAGIC companies = ["GOOGL", "AAPL", "MSFT", "TSLA"]
// MAGIC
// MAGIC # Initialize a Spark session
// MAGIC spark = SparkSession.builder.appName("StockPriceAnalysis").getOrCreate()
// MAGIC
// MAGIC # Loop through each company and fetch data
// MAGIC for company in companies:
// MAGIC     params = {
// MAGIC         "function": "TIME_SERIES_DAILY",
// MAGIC         "symbol": company,
// MAGIC         "apikey": api_key,
// MAGIC         "datatype":"json",
// MAGIC         "outputsize":"compact"
// MAGIC     }
// MAGIC     
// MAGIC     headers = {
// MAGIC         "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com",
// MAGIC         "X-RapidAPI-Key": "4a3c8ab6f6mshf33c91629f0ecd6p194a71jsn0810a5a1a065"
// MAGIC     }
// MAGIC     
// MAGIC     response = requests.get(base_url, params=params, headers=headers)
// MAGIC     data = response.json()
// MAGIC
// MAGIC
// MAGIC     #print(data)

// COMMAND ----------

// MAGIC %python
// MAGIC  # Convert JSON data to a PySpark DataFrame
// MAGIC
// MAGIC from pyspark.sql.functions import col, to_date, year, weekofyear, max
// MAGIC stock_data = [(date, float(values["1. open"]), float(values["2. high"]), float(values["3. low"]), float(values["4. close"]), int(values["5. volume"]))                  for date, values in data["Time Series (Daily)"].items()]
// MAGIC
// MAGIC columns = ["date", "open", "high", "low", "close", "volume"]
// MAGIC stock_df = spark.createDataFrame(stock_data, columns)
// MAGIC     
// MAGIC # Convert date column to a proper date type
// MAGIC stock_df = stock_df.withColumn("date", to_date(col("date")))
// MAGIC
// MAGIC # Create a list to store DataFrames for each company
// MAGIC #dataframes = []
// MAGIC #dataframes.append(stock_df)
// MAGIC
// MAGIC # Union all DataFrames to create a single DataFrame
// MAGIC #combined_df = dataframes[0].union(dataframes[1:])
// MAGIC #combined_df = dataframes
// MAGIC
// MAGIC stock_df = stock_df.withColumn("year", year(col("date")))
// MAGIC stock_df = stock_df.withColumn("week", weekofyear(col("date")))
// MAGIC
// MAGIC # Group by year and week and find the maximum closing price
// MAGIC weekly_max_closing = stock_df.groupBy("year", "week", "close").agg(max("close").alias("max_close")).orderBy("week", ascending= False)
// MAGIC
// MAGIC # Show the result
// MAGIC weekly_max_closing.show()

// COMMAND ----------

// MAGIC %python
// MAGIC weekly_max_closing.write.parquet("/FileStore/tables/weekly_max_closing.parquet")

// COMMAND ----------

// MAGIC %python
// MAGIC spark.read.parquet("/FileStore/tables/weekly_max_closing.parquet").count()

// COMMAND ----------

// MAGIC %md
// MAGIC ## ETL Job Four: RDBMS
// MAGIC
// MAGIC
// MAGIC ### Extract
// MAGIC Extract RNA data from a public PostgreSQL database.
// MAGIC
// MAGIC - https://rnacentral.org/help/public-database
// MAGIC - Extract 100 RNA records from the `rna` table (hint: use `limit` in your sql)
// MAGIC - hint: use `spark.read.jdbc` https://docs.databricks.com/external-data/jdbc.html
// MAGIC
// MAGIC ### Transform
// MAGIC We want to load the data as it so there is no transformation required.
// MAGIC
// MAGIC
// MAGIC ### Load
// MAGIC Load the DF in to a managed table called, `rna_100_records`

// COMMAND ----------

//Extract
//Extract RNA data from a public PostgreSQL database. only 100 rows 

val rna_100_records = (spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs")
  .option("dbtable", "rna")
  .option("user", "reader")
  .option("password", "NWDMCE5xdipIjRrp")
  .load()
  .limit(100)
)

println ("Count is: " + rna_100_records.count())


//rna_100_records.printSchema()


// COMMAND ----------

//fetching all rows 
val rna_records = (spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs")
  .option("dbtable", "rna")
  .option("user", "reader")
  .option("password", "NWDMCE5xdipIjRrp")
  .load()
)

println ("TOTAL Count is: " + rna_records.count())

// COMMAND ----------

//Load the DF in to a managed table called, rna_100_records

rna_100_records.write.saveAsTable("rna_100_records_scala")


