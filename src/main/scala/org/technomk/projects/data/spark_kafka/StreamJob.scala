package org.technomk.projects.data.spark_kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, date_trunc}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.technomk.projects.data.spark_kafka.configs.{ConfluentKafkaConfig, DeltaLakeConfig, KafkaConfig}
import za.co.absa.abris.avro.functions.from_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

object StreamJob {
  // function to read data from Kafka
  def readKafka(spark: SparkSession,
                kafkaConfig: KafkaConfig): DataFrame = {

    // read data from Kafka in streaming manner
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.brokerList) // here there is no need to cast kafkaConfig to ConfluentKafkaConfig,
      .option("kafka.security.protocol", kafkaConfig.securityProtocol) // because here we set up Kafka and all required attributes are available in KafkaConfig trait
      .option("subscribe", kafkaConfig.topicName)
      .option("startingOffsets", kafkaConfig.startingOffsets)
      .option("maxOffsetsPerTrigger", kafkaConfig.maxOffsetsPerTrigger)
      .load
  }

  // function to apply transformations to data as like deserialize avro format
  def transformAvro(kafkaConf: KafkaConfig): DataFrame => DataFrame = { src =>
    // and here, in difference to readKafka function, we need explicit type of kafkaConfig
    val confluentKafkaConfig = kafkaConf match {
      case confluent: ConfluentKafkaConfig => confluent
      case _ => throw new Error("Wrong configuration type!")
    }

    // prepare Schema Registry configuration
    val commonRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> confluentKafkaConfig.topicName,
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> confluentKafkaConfig.schemaRegistryUrl
    )

    val keyRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_ID -> confluentKafkaConfig.keySchemaId
      // the rest configs like PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY and PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY are not needed for Topic naming strategy
    )

    val valueRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> confluentKafkaConfig.valueSchemaId
    )

    // deserialize avro data to Spark structure
    src.withColumn("loadhour",
      date_format(date_trunc("hour", current_timestamp()), "yyyy-MM-dd-HH")) // add processing Hour to DataFrame
      .withColumn("key", from_confluent_avro(col("key"), keyRegistryConfig))
      .withColumn("value", from_confluent_avro(col("value"), valueRegistryConfig))
  }

  def writeDelta(deltaConf: DeltaLakeConfig): DataFrame => StreamingQuery = {
    // write data to Delta Lake table in streaming manner (more info in OneNote: Work → Telefonica → AWS Big Data Stream → Delta Lake)
    src =>
      src.writeStream
        .outputMode(deltaConf.outputMode) // append mode refers to Spark Streaming (don't confuse with Batch append/overwrite)
        .format("delta")
        .trigger(Trigger.ProcessingTime(deltaConf.triggerProcessingInterval)) // not specific for Delta Lake, but in general for Spark Streaming
        .option("checkpointLocation", deltaConf.checkpointLocation) // while this checkpoint overloaded by Deta Lake framework in it's personal manner
        .partitionBy("loadhour") // partition data by column
        .start(deltaConf.tableLocation)
  }

  def main(spark: SparkSession,
           kafkaConfig: KafkaConfig,
           deltaLakeConfig: DeltaLakeConfig): Boolean = {
    // run the complete processing here
    val readDf = readKafka(spark, kafkaConfig)
    val transformDf = readDf.transform(transformAvro(kafkaConfig))
    val query = writeDelta(deltaLakeConfig)(transformDf)

    // wait for specified time (or infinite with no parameters) and then stop the stream to run compaction
    query.awaitTermination(1000 * 60 * 10) // 10 minutes
    query.stop()
    query.awaitTermination(1000 * 60 * 10) // wait until currently running micro-batch finishes (it should return earlier, but just to make sure)
  }
}
