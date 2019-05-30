// Databricks notebook source
import org.apache.spark.eventhubs._

var eventHubConnectionString="Endpoint=sb://manianalytics.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hD1J4DZp2eRi7KykWkw12VC22qxmeve1itijvv/tcKg="
var eventHubPath = "messages"

val connectionString = ConnectionStringBuilder(eventHubConnectionString).setEventHubName(eventHubPath).build

val ehConf = EventHubsConf(connectionString)

// COMMAND ----------

val df = spark.readStream.format("eventhubs").options(ehConf.toMap).load()

// COMMAND ----------

val eventhubs = df.select($"body" cast "string")

// COMMAND ----------

eventhubs.writeStream.outputMode("append").format("console").option("truncate","false").option("numRows",50).start

// COMMAND ----------

val consumer2 = eventhubs.writeStream.format("csv").option("format", "append").option("path", "/tmp/output").option("checkpointLocation", "/tmp/").outputMode("append").start()

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /tmp/output

// COMMAND ----------

val data = sc.textFile("/tmp/output/part-00000-a4c2f9a0-711a-4f92-86d9-bfdf0502aaca-c000.csv").collect


// COMMAND ----------

