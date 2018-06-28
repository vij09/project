package com.kloud9.k8s.metrics

import com.kloud9.common.utilities.logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, from_json, _}
import org.apache.spark.sql.types._
import com.samelamin.spark.bigquery._
import org.apache.spark.SparkConf


/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: PodMetrics <brokers> <topics>
  * <brokers> is a list of one or more Kafka brokers
  * <groupId> is a consumer group name to consume from topics
  * <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  * $ bin/run-example streaming.PodMetrics broker1-host:port,broker2-host:port \
  * consumer-group topic1,topic2
  */

object MemoryLimit {

  val jsonPath = "/Users/suman.c/Downloads/ninth-park-203402-c53110836b3e.json"

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: PodMetrics <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <groupId> is a consumer group name to consume from topics
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    logger.setStreamingLogLevels()

    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval

    val spark = SparkSession
      .builder()
      .appName("K8s_Cluster_Pod_Metrics_Puller")
      .config("es.index.auto.create", "true")
      .config("es.resource", "test")
      .config("es.nodes", "127.0.0.1")
      .config("es.output.json", "true")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    /* Kafka Stream Creation */
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "MemoryLimit")
      .option("checkpointLocation", "kloud9-big-query-testing")
      .option("failOnDataLoss", "false")
      .option("multiLine", true)
      .load()

    println("****************** After getting data from kafka **************")

    /* Extracting Metrics as Json from Value */

    val mySchema = new StructType()
      .add("Results",
        ArrayType(
          new StructType()
            .add("Series",
              ArrayType(
                new StructType()
                  .add("name", StringType)
                  .add("columns",
                    ArrayType(StringType))
                  .add("values",
                    ArrayType(ArrayType(StringType)))
              ))
        ))

    var m = ds1.select($"value" cast "string" as "json").select(from_json($"json", mySchema) as "data").select("data.*")

    m = m.select(explode('Results) as 'res).select(explode(col("res.Series")) as 'ser).select(explode(col("ser.values")) as 'val)
    m = m.withColumn("time", $"val"(0))
      .withColumn("cluster_name", $"val"(1))
      .withColumn("container_base_image", $"val"(2))
      .withColumn("container_name", $"val"(3))
      .withColumn("labels", $"val"(4))
      .withColumn("namespace_name", $"val"(5))
      .withColumn("nodename", $"val"(6))
      .withColumn("pod_name", $"val"(7))
      .withColumn("schedulable", $"val"(8))
      .withColumn("type", $"val"(9))
      .withColumn("value", $"val"(10))


    m = m.drop(col("val"))
    m.printSchema()

    println("Data getting streamed : ",ds1.isStreaming)

    val sqlContext = spark.sqlContext

    // Set up GCP credentials
    sqlContext.setGcpJsonKeyFile(jsonPath) // Service account with Big Query Admin and Storage Admin roles
    sqlContext.setBigQueryProjectId("ninth-park-203402") // Project ID of the current project
    sqlContext.setBigQueryGcsBucket("kloud9-big-query-testing") // Bucket name created in the above project
    sqlContext.setBigQueryDatasetLocation("US") // Data set location

    println("************* Start streaming the data ************")
    m.writeStream
      .option("checkpointLocation", "kloud9-big-query-testing")
      .option("tableReferenceSink","ninth-park-203402:k9.influxDataMemoryLimit")
      .outputMode("append")
      .format("com.samelamin.spark.bigquery")
      .start().awaitTermination()
    println("************** Done writing to Big Query ****************")



    val query = m.writeStream
      .outputMode("append")
      .format("console")
      .start().awaitTermination()

    println("******************** This is the end **************************")


  }

}
