// Databricks notebook source
// MAGIC %md
// MAGIC 1. Pobierz dane Spark-The-Definitive_Guide dostępne na github
// MAGIC 2. Użyj danych do zadania '../retail-data/all/online-retail-dataset.csv'

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/online_retail_dataset.csv"
val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Zapisz DataFrame do formatu delta i stwórz dużą ilość parycji (kilkaset)
// MAGIC * Partycjonuj po Country

// COMMAND ----------

val path = "dbfs:/FileStore/tables/deltaFormat"
df.write.
    format("delta").
    partitionBy("Country").
    mode("overwrite").save(path)
val df2 = spark.read
  .format("delta").
  load(path).
repartition(250)

df2.rdd.getNumPartitions

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS tab")

spark.sql(s"""
  CREATE TABLE tab
  USING Delta
  LOCATION 'dbfs:/FileStore/tables/deltaFormat'
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1: OPTIMIZE and ZORDER
// MAGIC 
// MAGIC Wykonaj optymalizację do danych stworzonych w części I `../delta/retail-data/`.
// MAGIC 
// MAGIC Dane są partycjonowane po kolumnie `Country`.
// MAGIC 
// MAGIC Przykładowe zapytanie dotyczy `StockCode`  = `22301`. 
// MAGIC 
// MAGIC Wykonaj zapytanie i sprawdź czas wykonania. Działa szybko czy wolno 
// MAGIC 
// MAGIC Zmierz czas zapytania kod poniżej - przekaż df do `sqlZorderQuery`.

// COMMAND ----------

// TODO
def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

val sqlZorderQuery = timeIt(spark.sql("select * from tab where StockCode = '22301'").collect())

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from tab where StockCode = '22301'

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Skompaktuj pliki i przesortuj po `StockCode`.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- wypelnij
// MAGIC OPTIMIZE tab
// MAGIC ZORDER by (StockCode)

// COMMAND ----------

// MAGIC %md
// MAGIC Uruchom zapytanie ponownie tym razem użyj `postZorderQuery`.

// COMMAND ----------

// TODO
val poZorderQuery = timeIt(spark.sql("select * from tab where StockCode = '22301'").collect())

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2: VACUUM
// MAGIC 
// MAGIC Policz liczbę plików przed wykonaniem `VACUUM` for `Country=Sweden` lub innego kraju

// COMMAND ----------

// TODO
val plikiPrzed = dbutils.fs.ls("dbfs:/FileStore/tables/deltaFormat/Country=Sweden").length

// COMMAND ----------

// MAGIC %md
// MAGIC Teraz wykonaj `VACUUM` i sprawdź ile było plików przed i po.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC VACUUM tab

// COMMAND ----------

// MAGIC %md
// MAGIC Policz pliki dla wybranego kraju `Country=Sweden`.

// COMMAND ----------

// TODO
val plikiPo = dbutils.fs.ls("dbfs:/FileStore/tables/deltaFormat/Country=Sweden").length

// COMMAND ----------

// MAGIC %md
// MAGIC ## Przeglądanie histrycznych wartośći
// MAGIC 
// MAGIC możesz użyć funkcji `describe history` żeby zobaczyć jak wyglądały zmiany w tabeli. Jeśli masz nową tabelę to nie będzie w niej history, dodaj więc trochę danych żeby zoaczyć czy rzeczywiście się zmieniają. 

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history tab
