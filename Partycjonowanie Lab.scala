// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)


val fileName = "dbfs:/FileStore/tables/pageviews_by_second.tsv"

val data = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

data.write.mode("overwrite").parquet("dbfs:/pageviews_by_second.parquet")

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "dbfs:/pageviews_by_second.parquet"

val df = spark.read
  .parquet(parquetDir)
.repartition(2000)
//    .coalesce(6)
.groupBy("site").sum()


df.explain
df.count()


// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

import org.apache.log4j.{Level, Logger}

// COMMAND ----------

val actorsDfPath ="dbfs:/FileStore/tables/actors.txt"

val schema = StructType(
  List(
    StructField("imdb_title_id", StringType, true),
    StructField("ordering", IntegerType, false),
    StructField("imdb_name_id", StringType, false),
    StructField("job", StringType, false),
    StructField("characters", StringType, false),
  )
)

val data = spark.read
  .option("header", "true")
  .option("sep", ",")
  .schema(schema)
  .csv(actorsDfPath)
display(data)

// COMMAND ----------

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertions._

def loadDataFrame(actorsDfPath: String): DataFrame = {
  assert(actorsDfPath.endsWith(".txt"))
  Logger.getLogger("Assertion passed: actorsDfPath ends with .txt").setLevel(Level.INFO)
  
  val schema = StructType(
  List(
      StructField("imdb_title_id", StringType, true),
      StructField("ordering", IntegerType, false),
      StructField("imdb_name_id", StringType, false),
      StructField("job", StringType, false),
      StructField("characters", StringType, false),
    )
  )

  val df = spark.read
  .option("header", "true")
  .option("sep", ",")
  .schema(schema)
  .csv(actorsDfPath)
  Logger.getLogger("Dataset loaded").setLevel(Level.INFO)
  return df
}



// COMMAND ----------

import org.apache.spark.sql.Column
def checkColumnNullNumber(colName : Column, df :DataFrame) : Long ={
  if(df.columns.contains(colName))
      Logger.getLogger("Column exists").setLevel(Level.INFO)
  else
  {
      Logger.getLogger("No such column in dataset").setLevel(Level.ERROR)
      return 0
  }
  
  val count =df.filter(colName.isNull).count()
  
  return count
}

// COMMAND ----------

 import spark.implicits._
def countOccurances(colName : String, df :DataFrame) : DataFrame ={
  if(df.columns.contains(colName))
      Logger.getLogger("Column exists").setLevel(Level.INFO)
  else
  {
      Logger.getLogger("No such column in dataset").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  
  val dfCount =df.groupBy(colName).count()
  
  return dfCount
}

// COMMAND ----------

def replaceAll_WithWhitesace(column: String): Column = {
   if(df.columns.contains(column))
      Logger.getLogger("Column exists").setLevel(Level.INFO)
  else
  {
      Logger.getLogger("No such column in dataset").setLevel(Level.ERROR)
  }
  Logger.getLogger("All '_' signs replaced with whitespace").setLevel(Level.INFO)
  
  regexp_replace(col(column), "_", " ")
}

// COMMAND ----------

def removeTTFromIndex(df: DataFrame) : DataFrame = {
   if(df.columns.contains("imdb_title_id"))
      Logger.getLogger("Column exists").setLevel(Level.INFO)
  else
  {
      Logger.getLogger("No such column in dataset").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  Logger.getLogger("All '_' signs replaced with whitespace").setLevel(Level.INFO)
  
  val newDf = df.withColumn("imdb_title_id",  regexp_replace(col("imdb_title_id"), "tt", ""))
  return newDf
}

// COMMAND ----------

val actors = loadDataFrame(actorsDfPath)
display(actors)

// COMMAND ----------

checkColumnNullNumber(col("characters"),actors)

// COMMAND ----------

display(countOccurances("job",actors))

// COMMAND ----------

display(countOccurances("job",actors).withColumn("SpacesAddes",replaceAll_WithWhitesace("job")))

// COMMAND ----------

display(removeTTFromIndex(actors))
