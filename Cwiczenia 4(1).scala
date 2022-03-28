// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val names=tabela.where("TABLE_SCHEMA == 'SalesLT'").select("TABLE_NAME").as[String].collect.toList

for( i <- names){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()

  tab.write.format("delta").mode("overwrite").saveAsTable(i)
}


// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

// Nulls w kolumnach 

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when, count}

def countNullCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

for( i <- names.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")

  tab.select(countNullCols(tab.columns):_*).show()
  
}


// COMMAND ----------

//Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
import org.apache.spark.sql.functions.{col,when, count}

for( i <- names.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  val tabNew= tab.na.fill("0", tab.columns)
    display(tabNew)
}


// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle

for( i <- names.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  tab.na.drop("any").show(false)
}

// COMMAND ----------

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]

import spark.implicits._
import org.apache.spark.sql.functions.{avg,stddev,kurtosis}

val tab = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/salesorderheader")

  println("avg: "+
    tab.select(avg("Freight")).collect()(0)(0))

  println("stddev: "+
    tab.select(stddev("Freight")).collect()(0)(0))

  println("kurtosis: "+
    tab.select(kurtosis("TaxAmt")).collect()(0)(0))



// COMMAND ----------

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
//Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

import org.apache.spark.sql.functions._ 

val tab = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/product")


tab.groupBy("ProductModelId").
  agg( count(lit(1)).alias("Num Of ProductModelID")).sort(desc("Num Of ProductModelID")).show()



// COMMAND ----------

tab.groupBy("Color").agg(Map("Size" -> "mean", "Weight" -> "mean")).show()

// COMMAND ----------

tab.groupBy("ProductCategoryID")
  .agg(Map("StandardCost" -> "mean")).show()

// COMMAND ----------

display(tab)

// COMMAND ----------

tab.groupBy("Weight")
  .agg(Map("Size" -> "mean")).show()

// COMMAND ----------

// UDF
val customer = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/customer")

val convertSalesPerson =  (strQuote:String) => {
    val arr = strQuote.split("-")
    arr.map(f=>  f).mkString("_")
}
val convertUDF = udf(convertSalesPerson)

customer.select(col("SalesPerson"), 
    convertUDF(col("SalesPerson")).as("SalesPersonConverted") ).show(true)

// COMMAND ----------

val convertPriceToPln =  (price:Double) => {
   price*4.27.round
}
val convertUDF = udf(convertPriceToPln)flatten jsona


tab.select(col("ListPrice"), 
    convertUDF(col("ListPrice")).as("ListPriceConverted") ).show(true)


// COMMAND ----------

// UDF
val product = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")

val convertCustomerId =  (price:Int) => {
   price+500
}
val convertUDF = udf(convertCustomerId)

product.select(col("CustomerID"), 
    convertUDF(col("CustomerID")).as("CustomerIDConverted") ).show(true)
