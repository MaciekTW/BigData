// Databricks notebook source
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
// zadanie 1


val filePath = "dbfs:/FileStore/tables/Files/actors.csv"


val imdb_title = StructField("imdb_title_id", StringType, false)
val ordering = StructField("ordering", IntegerType, true)
val imdb_name = StructField("imdb_name_id", StringType, true)
val category = StructField("category", StringType, true)
val job = StructField("job", StringType, true)
val characters = StructField("characters", StringType, true)

val schema = StructType(Array(imdb_title,ordering,imdb_name,category,job,characters))

val actorsDf = spark.read.format("csv")
            .option("header","true")
            .schema(schema)
            .load(filePath)

display(actorsDf)

// COMMAND ----------

actorsDf.head(3)

// COMMAND ----------

val schema2 = "imdb_title_id INT, ordering INT, imdb_name_id INT, category STRING, job STRING, characters STRING"

// COMMAND ----------

//Zadanie 2

actorsDf.write.mode("overwrite").json("dbfs:/FileStore/tables/Files/actors.json")

val filePathJson = "dbfs:/FileStore/tables/Files/actors.json"
val jsonFile = spark.read.json(filePathJson)

print(jsonFile.schema)
jsonFile.show()

// COMMAND ----------

//zadanie 4
spark.read.format("csv").option("mode","PERMISSIVE").schema(schema).load(filePath)
spark.read.format("csv").option("mode","DROPMALFORMED").schema(schema).load(filePath)
spark.read.format("csv").option("mode","FAILFAST").schema(schema).load(filePath)
spark.read.format("csv").option("mode","FAILFAST").schema(schema).option("badRecordsPath", "dbfs:/FileStore/badRecordsPath").load(filePath)

// COMMAND ----------

// zadanie 5

val outputFileParquet = "dbfs:/FileStore/tables/Files/actors.parquet"
actorsDf.write.json(path=outputFileParquet)
spark.read.schema(schema).option("enforceSchema",true).parquet(path=outputFileParquet)



