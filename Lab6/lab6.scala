// Databricks notebook source
val transactionsDf = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)).toDF("AccountId", "TranDate", "TranAmt")


val logicalDf = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000)).toDF("RowID" ,"FName" , "Salary" )

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


val windowFun  = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val run_total=sum("TranAmt").over(windowFun)

transactionsDf.select(col("AccountId"), col("TranDate"), col("TranAmt") ,run_total.alias("RunTotalAmt") ).orderBy("AccountId").show()

// COMMAND ----------

val transactionDfWindows = transactionsDf.withColumn("RunTotalAmt",run_total)
.withColumn("RunAvg",avg("TranAmt").over(windowFun))
.withColumn("RunSmallAmt",min("TranAmt").over(windowFun))
.withColumn("RunLargeAmt",max("TranAmt").over(windowFun))
.withColumn("RunTranQty",count("*").over(windowFun))
display(transactionDfWindows)

// COMMAND ----------

val windowFunRange  = windowFun.rowsBetween(-2, Window.currentRow) 

val transactionDfWindowsRange = transactionsDf.withColumn("SlideTotal",run_total)
.withColumn("SlideAvg",avg("TranAmt").over(windowFun))
.withColumn("SlideMin",min("TranAmt").over(windowFun))
.withColumn("SlideMax",max("TranAmt").over(windowFun))
.withColumn("SlideQty",count("*").over(windowFun))
.withColumn("RN",row_number().over(windowFun))
display(transactionDfWindows)

// COMMAND ----------


val windowFunRows  = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow) 
val  windowSpecRange  = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow) 

val logicalDfWindows = logicalDf.withColumn("SumByRows",sum(col("Salary")).over(windowFunRows)).withColumn("SumByRange",sum(col("Salary")).over(windowSpecRange))
display(logicalDfWindows)

// COMMAND ----------

val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")

val windowFun  = Window.partitionBy("AccountNumber").orderBy("OrderDate")
val tabWindow=tab.select(col("AccountNumber"), col("OrderDate"), col("TotalDue") ,row_number().over(windowFun).alias("RN") ).orderBy("AccountNumber").limit(10)
display(tabWindow)

// COMMAND ----------

display(tab)

// COMMAND ----------

//Zadanie 2
val windowFun =Window.partitionBy(col("Status")).orderBy("OrderDate")
val windowFunRows=windowFun.rowsBetween(Window.unboundedPreceding, -2) 


val tabRows = tab.withColumn("lead",lead(col("SubTotal"), 2).over(windowFun))
.withColumn("lag",lag(col("SubTotal"),1).over(windowFun))
.withColumn("last",last(col("SubTotal")).over(windowFunRows))
.withColumn("first",first(col("SubTotal")).over(windowFunRows))
.withColumn("RN",row_number().over(windowFun))
.withColumn("DR",dense_rank().over(windowFun))


display(tabRows)

// COMMAND ----------

// Zadanie 3


// COMMAND ----------

val tabDetails = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderdetail")


val joinLeft = tab.join(tabDetails, tab.col("SalesOrderID") === tabDetails.col("SalesOrderID"), "leftsemi")
joinLeft.show()

// COMMAND ----------

joinLeft.explain()

// COMMAND ----------

val joinLeftAnti = tab.join(tabDetails, tab.col("SalesOrderID") === tabDetails.col("SalesOrderID"), "leftanti")
joinLeftAnti.show()

// COMMAND ----------

joinLeftAnti.explain()

// COMMAND ----------

//Zaadanie 4

// COMMAND ----------

display(joinLeft)

// COMMAND ----------

display(joinLeft.distinct())

// COMMAND ----------

display(joinLeft.dropDuplicates())

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val joinBroadcast = tab.join(broadcast(tabDetails), tab.col("SalesOrderID") === tabDetails.col("SalesOrderID"))
joinBroadcast.show()

// COMMAND ----------

joinBroadcast.explain()
