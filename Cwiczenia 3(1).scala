// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
var namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._

val namesDfWithTime = namesDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp")).explain

// COMMAND ----------

// Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._

val newColumn = namesDf.withColumn("feet",col("height")*0.3937007874)
display(newColumn)

// COMMAND ----------

// Odpowiedz na pytanie jakie jest najpopularniesze imię?

import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.expressions._

val nameDf2 = namesDf.withColumn("extractedName", split(col("name")," ").getItem(0))

nameDf2.groupBy("extractedName").count().orderBy($"count".desc).first()

// COMMAND ----------

//Dodaj kolumnę i policz wiek aktorów 

import org.apache.spark.sql.functions.{col, to_date,when,floor,datediff,lit,coalesce}
import spark.implicits._

val convertedDates = namesDf.withColumn("birthYear", regexp_extract(col("date_of_birth"),"[0-9][0-9][0-9][0-9]",0)).withColumn("deathYear", when(col("date_of_death").isNull,lit(2022)).otherwise(regexp_extract(col("date_of_death"),"[0-9][0-9][0-9][0-9]",0)))
val finalDf = convertedDates.withColumn("Age", col("deathYear")-col("birthYear"))

display(finalDf)


// COMMAND ----------

// Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
for( i <- 0 to namesDf.columns.size - 1) 
{
    namesDf = namesDf.withColumnRenamed(namesDf.columns(i), namesDf.columns(i).toUpperCase.replaceAll ("_", ""))
}
display(namesDf)

// COMMAND ----------

// Usuń kolumny (bio, death_details)
val nameDfDropped = namesDf.drop("bio").drop("death_details") 
display(nameDfDropped)

// COMMAND ----------

// Posortuj dataframe po imieniu rosnąco
val sortedDf=nameDf2.orderBy($"name")
display(sortedDf)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._

val namesDfWithRunTime = namesDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp"))

// COMMAND ----------

//Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
year(current_date())
val namesDfMovieAge = namesDf.withColumn("age", year(current_date())-col("year"))
display(namesDfMovieAge)

// COMMAND ----------

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut) 
val namesDfMovieNumericBudget = namesDf.withColumn("budget_numeric", regexp_replace(col("budget"),"[$a-zA-Z]+",""))
display(namesDfMovieNumericBudget)


// COMMAND ----------

//Usuń wiersze z dataframe gdzie wartości są null

display(namesDf.na.drop())

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

var filledDf = namesDf
  .where($"value".isNotNull)
  .withColumnRenamed("value", "recent_value")

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._

val namesDfWithRunTime = namesDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp"))

// COMMAND ----------

//Dla każdego z poniższych wyliczeń nie bierz pod uwagę nulls

// COMMAND ----------

//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.functions.split


var dfWithMedianAndMean = namesDf.withColumn("mean",(col("votes_1")+col("votes_2")*lit(2)+col("votes_3")*lit(3)+col("votes_4")*lit(4)+col("votes_5")*lit(5)+col("votes_6")*lit(6)+col("votes_7")*lit(7)+col("votes_8")*lit(8)+col("votes_9")*lit(9)+col("votes_10")*lit(10)).cast(DoubleType)/(col("total_votes")))


var median = namesDf.stat.approxQuantile("votes_1", Array(0.5), 0.25)

print(median)


//display(dfWithMedianAndMean)

// COMMAND ----------

//Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
val namesDfWithRunTime = namesDf.withColumn("meanDiff",col("weighted_average_vote")-col("mean_vote")).withColumn("medianDiff",col("weighted_average_vote")-col("median_vote"))

display(namesDfWithRunTime)

// COMMAND ----------

//Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
namesDf.describe("males_allages_avg_vote").show()


// COMMAND ----------

namesDf.describe("females_allages_avg_vote").show()

// COMMAND ----------

//Dla jednej z kolumn zmień typ danych do long

import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.sql.functions.split

var dfWithLong = namesDf.select(col("total_votes").cast(LongType))

// COMMAND ----------

// zadanie 3 Do jednej z Dataframe dołóż transformacje groupBy i porównaj jak wygląda plan wykonania 

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

moviesDf.select($"genre",$"year").explain(true)



// COMMAND ----------

moviesDf.select($"genre",$"year").groupBy("genre").count().explain(true)

// COMMAND ----------

// zadanie 4

var username = "sqladmin"
var password = "$3bFHs56&o123$" 

val dataFromSqlServer = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT table_name FROM information_schema.tables) tmp")
      .option("user", username)
      .option("password",password)
      .load()

display(dataFromSqlServer)

