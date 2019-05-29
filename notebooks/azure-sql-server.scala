// Databricks notebook source
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._


// COMMAND ----------

val config = Config(Map(
  "url"            -> "mani-analytics-dbserver.database.windows.net",
  "databaseName"   -> "mani-analytics-db",
  "dbTable"        -> "dbo.Clients",
  "user"           -> "admin123",
  "password"       -> "R00tr00t",
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

// COMMAND ----------

val collection = spark.read.sqlDB(config)


// COMMAND ----------

collection.printSchema



// COMMAND ----------

collection.printSchema

display(collection)

// COMMAND ----------

// DBTITLE 1,Bulk copy
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

// COMMAND ----------

var bulkCopyMetadata = new BulkCopyMetadata
bulkCopyMetadata.addColumnMetadata(1, "Title", java.sql.Types.NVARCHAR, 10, 0)
bulkCopyMetadata.addColumnMetadata(2, "FirstName", java.sql.Types.NVARCHAR, 100, 0)
bulkCopyMetadata.addColumnMetadata(3, "LastName", java.sql.Types.NVARCHAR, 100, 0)


// COMMAND ----------

val bulkCopyConfig = Config(Map(
  "url"               -> "mani-analytics-dbserver.database.windows.net",
  "databaseName"      -> "mani-analytics-db",
  "user"              -> "admin123",
  "password"          -> "R00tr00t",
  "dbTable"           -> "dbo.CustomerNames",
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// COMMAND ----------

val customerSchema = List(
  StructField("Title", StringType, true),
  StructField("FirstName", StringType, true),
  StructField("LastName", StringType, true)
)

// COMMAND ----------

val data = sc.textFile("dbfs:///mnt/salesdata1/customer-names.csv").
  mapPartitionsWithIndex((index, iterator) => {
    if(index == 0) iterator.drop(1)
    
    iterator
  }).map(line => {
    val splitted = line.split(",")
    
    Row(splitted(0), splitted(1), splitted(2))
  })

// COMMAND ----------

val customersFrame = spark.createDataFrame(data, StructType(customerSchema))

// COMMAND ----------

display(customersFrame)

// COMMAND ----------

customersFrame.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)

// COMMAND ----------

