package org.technomk.projects.data.spark_kafka

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{ArrayType, BooleanType, StringType, StructField, StructType}
import org.technomk.projects.data.spark_kafka.configs.{ApplicationConfig, Configurations, ConfluentKafkaConfig, KafkaConfig}
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager

object TestSparkKafkaAvroSchemaRegistryUtils {

  // below is the hierarchy of DataTye (StructType) for the DataFrame
  // it must conform 1-to-1 to corresponding avro schema
  val keyType: StructType = StructType(List(
    StructField("spId", StringType, nullable = false),
    StructField("lcId", StringType, nullable = false)
  ))

  val featureItemType: StructType = StructType(List(
    StructField("id", StringType, nullable = false),
    StructField("granted", BooleanType, nullable = false)
  ))

  val consentItemMetaType: StructType = StructType(List(
    StructField("channel", StringType, nullable = false),
    StructField("username", StringType, nullable = false),
    StructField("usertype", StringType, nullable = true)
  ))

  val consentActionItemType: StructType = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("submitDate", StringType, nullable = false)
  ))

  val consentItemType: StructType = StructType(List(
    StructField("date", StringType, nullable = false),
    StructField("granted", BooleanType, nullable = false),
    StructField("meta", consentItemMetaType, nullable = false),
    StructField("configId", StringType, nullable = false),
    StructField("versionId", StringType, nullable = false),
    StructField("channels", ArrayType(featureItemType, containsNull = false), nullable = true),
    StructField("dataTypes", ArrayType(featureItemType, containsNull = false), nullable = true),
    StructField("purposeTypes", ArrayType(featureItemType, containsNull = false), nullable = true),
    StructField("productTypes", ArrayType(featureItemType, containsNull = false), nullable = true)
  ))

  val consentEventType: StructType = StructType(List(
    StructField("spId", StringType, nullable = false),
    StructField("lcId", StringType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("current", ArrayType(consentItemType, containsNull = false), nullable = false),
    StructField("previous", ArrayType(consentItemType, containsNull = false), nullable = true),
    StructField("action", ArrayType(consentActionItemType, containsNull = false), nullable = true)
  ))

  val keyValueType: StructType = StructType(List(
    StructField("key", keyType, nullable = false),
    StructField("value", consentEventType, nullable = false)
  ))

  // this is the default schema to read string-encoded JSONs data from CSV file
  val keyValueFileType: StructType = StructType(List(
    StructField("key", StringType, nullable = false),
    StructField("value", StringType, nullable = false)
  ))


  // this function reads JSON data from CSV file and applies specified schema
  def readMessageFromCsv(spark: SparkSession, dataPath: String): DataFrame = {
    val init = spark.read
      .format("csv")
      .option("header", value = false)
      .schema(keyValueFileType)
      .load(dataPath)
      .select(
        from_json(col("key"), keyType).as("key"),
        from_json(col("value"), consentEventType).as("value")
      )

    // when reading from file all fields are set to be nullable (SPARK-10848 for details), which doesn't
    // conform to specified avro schema, so need to re-apply schema to DataFrame
    init.sqlContext.createDataFrame(init.rdd, keyValueType)

    // in general, when serialize/deserialize data to/from AVRO format, it is required that
    // avro schema totally conforms to DataFrame Spark schema (StructType)
  }


  // this function read data from Delta Lake table
  def readDataFromDelta(spark: SparkSession, dataPath: String): DataFrame = {
    // read from delta lake
    val result = spark.read
      .format("delta")
      .load(dataPath)
      .select("key", "value")

    // when reading from file all fields are set to be nullable (SPARK-10848 for details), which doesn't
    // conform to specified avro schema, so need to re-apply schema to DataFrame (also required for except function used in assert)
    result.sqlContext.createDataFrame(result.rdd, keyValueType)
  }


  // this function writes data into Kafka in avro-encoded format
  // additional step is to register corresponding avro schemas in schema registry
  def writeAvroMessageToKafka(dataFrame: DataFrame, kafkaConfig: KafkaConfig): Unit = {
    // get kafkaConfig of required type (another approach to the one specified in readFakeConfigs function)
    val confluentKafkaConfig = kafkaConfig.asInstanceOf[ConfluentKafkaConfig]

    // prepare Schema Registry configuration
    val commonRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> confluentKafkaConfig.topicName,
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> confluentKafkaConfig.schemaRegistryUrl
    )
    val keyRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME
    )
    val valueRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME
    )

    // open up a connection to Schema Registry
    SchemaManager.configureSchemaRegistry(commonRegistryConfig)

    // read schemas
    val keySchema = AvroSchemaUtils.load("src/main/resources/avro/key.avsc")
    val valueSchema = AvroSchemaUtils.load("src/main/resources/avro/value.avsc")

    // there is alternative approach to get avro schema programmatically instead of manually written (however, it doesn't work in some cases)
    //    val avroSchema = SparkAvroConversions.toAvroSchema(sqlSchema, avro_schema_name, avro_schema_namespace)

    // register schemas in Schema Registry (this is standard subject naming for Topic naming strategy)
    SchemaManager.register(keySchema, confluentKafkaConfig.topicName + "-key")
    SchemaManager.register(valueSchema, confluentKafkaConfig.topicName + "-value")

    // convert to avro and write to Kafka
    dataFrame.select(
      to_confluent_avro(col("key"), keyRegistryConfig).as("key"),
      to_confluent_avro(col("value"), valueRegistryConfig).as("value")
    )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", confluentKafkaConfig.brokerList)
      .option("topic", confluentKafkaConfig.topicName)
      .save
  }

  // this function reads configuration file, translates it to configuration classes hierarchy
  def readFakeConfigs(spark: SparkSession,
                      kafkaPort: Int,
                      schemaRegistryPort: Int,
                      triggerInterval: String,
                      testDirectory: String): ApplicationConfig = {
    // specify path to configuration file
    val applicationConfigPath = "src/main/resources/configurations/application.conf"

    // read configuration file and build configuration classes hierarchy
    val initialConfig = Configurations.loadConfig(applicationConfigPath)

    // every case class has default copy function to copy the object with some values updated (if value is not updated, it is inherited from initial object)
    // initialConfig is of type ApplicationConfig, which has KafkaConfig as attribute, which is  a trait and can't be copied, so here is how to do it:
    val fakeKafkaConfig = initialConfig.kafkaConfig match {
      case confluent: ConfluentKafkaConfig => confluent
        .copy(
          brokerList = s"localhost:$kafkaPort", // set actual Kafka port from EmbeddedKafka
          schemaRegistryUrl = s"http://localhost:$schemaRegistryPort",
          maxOffsetsPerTrigger = "1000" // the rest attributes stayed the same as in initialConfig
        )
      case _ => throw new Error("Wrong configuration type!")
    }

    // HDFS log storage is default for delta lake, but can be used AWS S3 or Azure Storage
    val fakeDeltaSparkConf = initialConfig.deltaConfig.deltaSparkConfig.copy(
      deltaLogStoreClassPath = Option("org.apache.spark.sql.delta.storage.HDFSLogStore"))

    // Delta Lake requires to apply custom config params to Spark
    // toMap function need to be defined to set up correct Spark configuration option names
    fakeDeltaSparkConf.toMap.foreach(mapEntry => spark.conf.set(mapEntry._1, mapEntry._2))

    // update some Delta Config attributes, same as for ConfluentKafkaConfig, but match is not requires
    val fakeDeltaConfig = initialConfig.deltaConfig.copy(
      checkpointLocation = s"$testDirectory/checkpoint/tmp-${UUID.randomUUID().toString}",
      tableLocation = s"$testDirectory/delta/out/",
      deltaSparkConfig = fakeDeltaSparkConf,
      triggerProcessingInterval = triggerInterval
    )

    // finally, return resulting ApplicationConfig object
    initialConfig.copy(kafkaConfig = fakeKafkaConfig, deltaConfig = fakeDeltaConfig)
  }

}
