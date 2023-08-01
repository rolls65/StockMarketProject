package org.itc.stockmarket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object StockMarketKafkaData extends App{
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkKafkaExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.format("kafka").option("kafka.bootstrap.servers","ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092").option("startingOffset","earliest").option("subscribe","stockdata").load()
  df.printSchema()
  //df.show(false)
  //org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;

  val schema = new StructType()
    .add("trading_year", IntegerType, true)
    .add("trading_month", IntegerType, true)
    .add("company_code", StringType, true)
    .add("comapany_name", StringType, true)
    .add("headquarters", StringType, true)
    .add("sector", StringType, true)
    .add("sub_industry", StringType, true)
    .add("opening", DoubleType, true)
    .add("closing", DoubleType, true)
    .add("low", DoubleType, true)
    .add("high", DoubleType, true)
    .add("volume", DoubleType, true)

  val stockdf = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value1")
  val stockdatadf = stockdf
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
  stockdatadf.printSchema()
  //person.show(false)
  /**
   * uncomment below code if you want to write it to console for testing.

  person.writeStream
        .format("console")
        .outputMode("update")
        .start()*/

  stockdatadf.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "chkpoint-location6")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()
    .awaitTermination()

  /**
   * uncomment below code if you want to write it to kafka topic.


  person.selectExpr("id AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "ip-172-31-13-101.eu-west-2.compute.internal:9092, ip-172-31-3-80.eu-west-2.compute.internal:9092, ip-172-31-5-217.eu-west-2.compute.internal:9092, ip-172-31-9-237.eu-west-2.compute.internal:9092")
    .option("topic", "stockdata")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation",  "checkpoint")
    .format("console")
    .start()
    .awaitTermination() */
}
