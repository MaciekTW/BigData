// Databricks notebook source
val catalog = spark.catalog
spark.sql("create database newDB")
display(spark.catalog.listDatabases().select("name"))

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val data1 = Seq(("Java", "1"), ("Python", "4"), ("Scala", "5"))

val data2 = Seq(("Apple", "4"), ("Orange", "3"), ("Peach", "5"))

val rdd = spark.sparkContext.parallelize(data1)
val rdd2 = spark.sparkContext.parallelize(data2)
                
val programming = rdd.toDF("language","rating")
val fruits = rdd2.toDF("fruit","rating")
                
                

programming.write.mode("overwrite").saveAsTable("newDB.programming")
fruits.write.mode("overwrite").saveAsTable("newDB.fruits")


// COMMAND ----------

catalog.listTables("newDB").show()

// COMMAND ----------

val tables = catalog.listTables().select("name").as[String].collect.toList



// COMMAND ----------


for( i <- tables){
    spark.sql(s"DELETE FROM newDB.$i")
  }

// COMMAND ----------


