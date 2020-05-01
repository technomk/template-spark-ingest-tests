package org.technomk.projects.data.spark_kafka.configs

import java.nio.file.Paths

import pureconfig._
import pureconfig.generic.auto._

// object to load configurations
object Configurations {
  val appNamespace: String = "application" // configuration namespace - the root element in .config file

  def loadConfig(path: String): ApplicationConfig = {
    pureconfig.loadConfigFromFiles[ApplicationConfig](
      Seq(Paths.get(path)),
      failOnReadError = false,
      appNamespace) match {
      case Right(config) => config
      case Left(configReaderFailures) =>
        throw new Exception(
          s"Encountered the following errors reading the configuration: ${configReaderFailures.toList.mkString("\n")}"
        )
    }
  }
}

// all-in-one general config trait and class
// names of parameters must conform to configuration file: paramName <=> param-name
// the same for "type" attribute in configuration file: type:"confluent-kafka-config" => ConfluentKafkaConfig class
sealed trait Config {
  def applicationGeneralConfig: GeneralConfig

  def kafkaConfig: KafkaConfig

  def deltaConfig: DeltaLakeConfig
}

case class ApplicationConfig(applicationGeneralConfig: GeneralConfig,
                             kafkaConfig: KafkaConfig,
                             deltaConfig: DeltaLakeConfig)
  extends Config


// sub-configuration class
final case class GeneralConfig(applicationName: String)


// sub-configuration class (multiple implementations of KafkaConfig are possible)
// so need to explicitly specify "type" value in configuration file
// (this is used as an example, but it is more suitable to use concrete case classes instead)
sealed trait KafkaConfig{
  def brokerList: String
  def securityProtocol: String
  def topicName: String
  def startingOffsets: String
  def maxOffsetsPerTrigger: String
}

final case class ConfluentKafkaConfig(brokerList: String,
                                      securityProtocol: String,
                                      topicName: String,
                                      schemaRegistryUrl: String,
                                      keySchemaId: String,
                                      valueSchemaId: String,
                                      startingOffsets: String,
                                      maxOffsetsPerTrigger: String)
  extends KafkaConfig

final case class SimpleKafkaConfig(brokerList: String,
                                   securityProtocol: String,
                                   topicName: String,
                                   startingOffsets: String,
                                   maxOffsetsPerTrigger: String)
  extends KafkaConfig

// sub-configuration class
// this class will have one more level of nested sub-class DeltaLakeSparkConfig
case class DeltaLakeConfig(outputMode: String,
                           triggerProcessingInterval: String,
                           checkpointLocation: String,
                           tableLocation: String,
                           deltaSparkConfig: DeltaLakeSparkConfig)

// 2nd level sub-configuration class
case class DeltaLakeSparkConfig(appendOnlyMode: Boolean,
                                enableExpiredLogCleanup: Boolean,
                                logRetentionDurationInterval: String,
                                deltaLogStoreClassPath: Option[String],
                                checkpointInterval: Option[Int]) {
  def toMap: Map[String, String] = {
    Map("spark.databricks.delta.properties.defaults.logRetentionDuration" -> logRetentionDurationInterval,
      "spark.databricks.delta.properties.defaults.appendOnly" -> appendOnlyMode.toString,
      "spark.databricks.delta.properties.defaults.enableExpiredLogCleanup" -> enableExpiredLogCleanup.toString,
      "spark.databricks.delta.properties.defaults.checkpointInterval" -> checkpointInterval.getOrElse(10).toString,
      "spark.delta.logStore.class" -> deltaLogStoreClassPath.getOrElse("org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"))
  }
}