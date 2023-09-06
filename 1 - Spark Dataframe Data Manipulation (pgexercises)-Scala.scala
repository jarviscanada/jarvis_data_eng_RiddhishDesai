// Databricks notebook source
// MAGIC %md
// MAGIC # Learning Objectives
// MAGIC In this notebook, you will learn Spark Dataframe APIs.
// MAGIC
// MAGIC # Question List
// MAGIC
// MAGIC Solve the following questions using Spark Dataframe APIs
// MAGIC
// MAGIC ### Join
// MAGIC
// MAGIC 1. easy - https://pgexercises.com/questions/joins/simplejoin.html
// MAGIC 2. easy - https://pgexercises.com/questions/joins/simplejoin2.html
// MAGIC 3. easy - https://pgexercises.com/questions/joins/self2.html 
// MAGIC 4. medium - https://pgexercises.com/questions/joins/threejoin.html (three join)
// MAGIC 5. medium - https://pgexercises.com/questions/joins/sub.html (subquery and join)
// MAGIC
// MAGIC ### Aggregation
// MAGIC
// MAGIC 1. easy - https://pgexercises.com/questions/aggregates/count3.html Group by order by
// MAGIC 2. easy - https://pgexercises.com/questions/aggregates/fachours.html group by order by
// MAGIC 3. easy - https://pgexercises.com/questions/aggregates/fachoursbymonth.html group by with condition 
// MAGIC 4. easy - https://pgexercises.com/questions/aggregates/fachoursbymonth2.html group by multi col
// MAGIC 5. easy - https://pgexercises.com/questions/aggregates/members1.html count distinct
// MAGIC 6. med - https://pgexercises.com/questions/aggregates/nbooking.html group by multiple cols, join
// MAGIC
// MAGIC ### String & Date
// MAGIC
// MAGIC 1. easy - https://pgexercises.com/questions/string/concat.html format string
// MAGIC 2. easy - https://pgexercises.com/questions/string/case.html WHERE + string function
// MAGIC 3. easy - https://pgexercises.com/questions/string/reg.html WHERE + string function
// MAGIC 4. easy - https://pgexercises.com/questions/string/substr.html group by, substr
// MAGIC 5. easy - https://pgexercises.com/questions/date/series.html generate ts
// MAGIC 6. easy - https://pgexercises.com/questions/date/bookingspermonth.html extract month from ts

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question
// MAGIC
// MAGIC How can you produce a list of the start times for bookings by members named 'David Farrell'?
// MAGIC
// MAGIC https://pgexercises.com/questions/joins/simplejoin.html

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.DataFrame
// MAGIC val bk = spark.sql("select * from booking_Scala")
// MAGIC bk.show(5)
// MAGIC val mem= spark.sql("SELECT* FROM members_Scala")
// MAGIC mem.show(5)
// MAGIC val mem1= spark.sql("SELECT* FROM members_Scala")
// MAGIC mem1.show(5)
// MAGIC val fac= spark.sql("select * from facilities_Scala")
// MAGIC fac.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Join

// COMMAND ----------

// MAGIC %scala
// MAGIC //How can you produce a list of the start times for bookings by members named 'David Farrell'?
// MAGIC
// MAGIC
// MAGIC val david= bk.join(mem, bk("memid")=== mem("memid"), "inner").select(bk("starttime"))
// MAGIC
// MAGIC val dav= david.filter(
// MAGIC   (mem("surname")==="Farrell") && (mem("firstname")=== "David"))
// MAGIC dav.show()

// COMMAND ----------

// MAGIC %scala
// MAGIC //How can you produce a list of the start times for bookings for tennis courts, for the date '2012-09-21'? Return a list of start time and facility name pairings, ordered by the time.
// MAGIC
// MAGIC val lis = List("Tennis Court 1", "Tennis Court 2")
// MAGIC
// MAGIC val tennis= bk.join(fac, bk("facid") === fac("facid") , "inner")
// MAGIC   .select(bk("starttime").alias("Time"),fac("name"))
// MAGIC val filteredTennis= tennis.filter(
// MAGIC   (bk("starttime") >="2012-09-21") && (bk("starttime") < "2012-09-22") && (fac("name").isin(lis: _* ))).orderBy(bk("starttime"))
// MAGIC
// MAGIC filteredTennis.show()
// MAGIC

// COMMAND ----------

//How can you output a list of all members, including the individual who recommended them (if any)? Ensure that results are ordered by (surname, firstname).

val recommender= mem.join(mem1, mem1("memid")===mem("recommendedby"), "left").
select(mem("firstname").alias("memfname"),mem("surname").alias("memsname"),mem1("firstname").alias("recfname"),mem1("surname").alias("recsname"))
.orderBy("memsname","memfname")
recommender.show()

// COMMAND ----------

//How can you produce a list of all members who have used a tennis court? Include in your output the name of the court, and the name of the member formatted as a single column. Ensure no duplicate data, and order by the member name followed by the facility name.

//from pyspark.sql.functions import concat,concat_ws,asc,desc
import org.apache.spark.sql.functions._

val li= List("Tennis Court 1","Tennis Court 2")

val data = mem.join(bk, mem("memid") === bk("memid"), "inner").join(fac, bk("facid") === fac("facid"), "inner")

val data1= data.select( concat(mem("firstname"), lit(" "),mem("surname")).alias("Name"),fac("name").alias("facility") )
.filter((fac("name").isin(li: _*)))

data1.distinct().orderBy("Name","facility").show()

// COMMAND ----------

// MAGIC %python
// MAGIC #SQL version for reference
// MAGIC
// MAGIC data1= spark.sql("select distinct m.firstname ||' '|| m.surname as member, f.name as facility from  members_Scala m inner join\
// MAGIC      booking_scala b on m.memid=b.memid inner join facilities_scala f on b.facid =f.facid \
// MAGIC          where f.name in ('Tennis Court 1','Tennis Court 2') order by member,facility asc")
// MAGIC data1.distinct().show()  

// COMMAND ----------

//How can you output a list of all members, including the individual who recommended them (if any), without using any joins? Ensure that there are no duplicates in the list, and that each firstname + surname pairing is formatted as a column and ordered.
import org.apache.spark.sql.functions._

mem.join(mem1, mem1("memid") === mem("recommendedby"), "left")
    .select(concat(mem("firstname"), lit(" "), mem("surname")).alias("member"),concat(mem1("firstname"),lit(" "), mem1("surname")).alias("recommender")).orderBy("member").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ###Aggregation

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC bk = spark.sql("select * from booking")
// MAGIC bk.show(5)
// MAGIC mem= spark.sql("SELECT* FROM members")
// MAGIC mem.show(5)
// MAGIC mem1= spark.sql("SELECT* FROM members")
// MAGIC mem1.show(5)
// MAGIC fac= spark.sql("select * from facilities")
// MAGIC fac.show(5)

// COMMAND ----------

//Produce a count of the number of recommendations each member has made. Order by member ID.

val rec= mem.groupBy("recommendedby").count().orderBy("recommendedby").filter(mem("recommendedby") > 0)
rec.show()

// COMMAND ----------

//Produce a list of the total number of slots booked per facility. For now, just produce an output table consisting of facility id and slots, sorted by facility id.

//bk.groupBy("facid").sum("slots").orderBy("facid").show()
val result = bk
  .groupBy("facid")
  .agg(sum("slots").alias("total_slots"))
  .orderBy("facid")

result.show()

// COMMAND ----------

//Produce a list of the total number of slots booked per facility in the month of September 2012. Produce an output table consisting of facility id and slots, sorted by the number of slots.
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val septbk= bk.filter(
    (bk("starttime") >= "2012-09-01") && (bk("starttime") < "2012-10-01")).groupBy("facid").agg(sum("slots").alias("Sum of Slots"))
septbk.show()


// COMMAND ----------

// MAGIC %python
// MAGIC #SQL for reference 
// MAGIC # 
// MAGIC data1= spark.sql ("select facid, sum(slots) from booking_Scala where starttime >= '2012-09-01'	and starttime < '2012-10-01'\
// MAGIC     	group by facid")
// MAGIC data1.show(5)

// COMMAND ----------

//Produce a list of the total number of slots booked per facility per month in the year of 2012. Produce an output table consisting of facility id and slots, sorted by the id and month.

val bwym = bk.withColumn("year", year(col("starttime"))).withColumn("month", month(col("starttime")))
val bwy= bwym.filter(col("year") === 2012).groupBy("facid","month").agg(sum("slots").alias("TotalSlots")).orderBy("facid","month")
bwy.show().toString

// COMMAND ----------

//Find the total number of members (including guests) who have made at least one booking.

val distinct_member_bookings = bk.groupBy("memid").agg(count("*").alias("count"))

// Filter for members with at least one booking
val members_with_bookings = distinct_member_bookings.where(col("count") >= 1)

//Get the total count of members with bookings
val total_members_with_bookings = members_with_bookings.count().toInt

print("Total members with at least one booking:", total_members_with_bookings)


// COMMAND ----------

//Produce a list of each member name, id, and their first booking after September 1st 2012. Order by member ID.
//from pyspark.sql.functions import min,max,col

val data = mem.join(bk, mem("memid") === bk("memid"), "inner").select(mem("surname"),mem("firstname"), mem("memid"), bk("starttime")).filter(bk("starttime") >= "2012-09-01")

data.groupBy("surname","firstname","memid").agg(min("starttime").alias("FirstBooking")).orderBy("memid").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ###String & Date

// COMMAND ----------

//Output the names of all members, formatted as 'Surname, Firstname'

//from pyspark.sql.functions import concat,concat_ws,asc,desc

val data= mem.select(concat(mem("surname"),lit(", "),mem("firstname")).alias("Full Name"))
data.show()

// COMMAND ----------

//Perform a case-insensitive search to find all facilities whose name begins with 'tennis'. Retrieve all columns.
import org.apache.spark.sql.functions._

val filtered_facilities = fac.filter(col("name").startsWith("Tennis"))
filtered_facilities.show()


// COMMAND ----------

//You've noticed that the club's member table has telephone numbers with very inconsistent formatting. You'd like to find all the telephone numbers that contain parentheses, returning the member ID and telephone number sorted by member ID.

//from pyspark.sql.functions import col, regexp_extract
import org.apache.spark.sql.functions._

//Define a regular expression to extract telephone numbers with parentheses
val telephone_pattern = "\\((\\d+)\\)"

//Extract telephone numbers using regexp_extract and create a new column 'telephone_number'
val members_with_telephone = mem.withColumn("telephone_number",regexp_extract(col("telephone"), telephone_pattern, 1))
    

//Filter rows where telephone numbers were found
val members_with_parentheses = members_with_telephone.filter(col("telephone_number") =!= "")

//Select and order by member ID and telephone number
val result_df = members_with_parentheses.select("memid", "telephone").orderBy("memid")

//Show the result
result_df.show()



// COMMAND ----------

//You'd like to produce a count of how many members you have whose surname starts with each letter of the alphabet. Sort by the letter, and don't worry about printing out a letter if the count is 0.


//from pyspark.sql.functions import substring,count

val data= mem.withColumn("letter", substring(col("surname"), 1,1))

data.groupBy("letter").agg(count(data("letter"))).orderBy(data("letter")).show()

// COMMAND ----------

// Create a DataFrame with a single column containing the dates in October 2012
val datesDF = spark.range(1, 32)
  .select(expr("concat('2012-10-', id) as date"))

// Show the list of dates
datesDF.select(expr("cast(date as timestamp) as timestamp")).show()


// COMMAND ----------

//Return a count of bookings for each month, sorted by month

val bwym = bk.withColumn("month", month(col("starttime")))

bwym.groupBy("month").count().orderBy("month").show()
