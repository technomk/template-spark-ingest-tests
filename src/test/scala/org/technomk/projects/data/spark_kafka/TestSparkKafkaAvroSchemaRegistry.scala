package org.technomk.projects.data.spark_kafka

import java.io.File

import com.holdenkarau.spark.testing.DatasetSuiteBase
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.abris.avro.read.confluent.SchemaManager

import scala.reflect.io.Directory

// this must be a class, not an object
class TestSparkKafkaAvroSchemaRegistry extends AnyFunSuite with DatasetSuiteBase {

  test("Test 1. Data is loaded from Kafka to Delta Lake table") {
    val testsCommonDir = "src/test/resources/test-data/"
    val testCurrentDir = "test-01/"

    // Step 1. Prepare source data - read from csv
    val in = TestSparkKafkaAvroSchemaRegistryUtils.readMessageFromCsv(spark = spark, s"${testsCommonDir + testCurrentDir}input")

    // Step 2. Configure and start embedded Kafka with Schema Registry
    // below configuration notifies EmbeddedKafka to use any available/free port for it's needs
    val embeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0,
      schemaRegistryPort = 0)

    // all services (Kafka, Zookeeper, Schema Registry) live only in scope of this block
    EmbeddedKafka.withRunningKafkaOnFoundPort(embeddedKafkaConfig) { actualConfig =>
      // actualConfig provides actual ports used by embedded services

      // Step 3. Read application configurations
      val fakeConfig = TestSparkKafkaAvroSchemaRegistryUtils.readFakeConfigs(
        spark,
        actualConfig.kafkaPort,
        actualConfig.schemaRegistryPort,
        triggerInterval = "20 seconds",
        testsCommonDir + testCurrentDir)

      // Step 4. Prepare source data - write to Kafka
      TestSparkKafkaAvroSchemaRegistryUtils.writeAvroMessageToKafka(in, fakeConfig.kafkaConfig)

      // Step 5. Run the streaming job to write data from Kafka to Delta table (for E2E test)
      val readDf = StreamJob.readKafka(spark, fakeConfig.kafkaConfig)
      val transformDf = readDf.transform(StreamJob.transformAvro(fakeConfig.kafkaConfig))
      val query = StreamJob.writeDelta(fakeConfig.deltaConfig)(transformDf)

      val awaitTime = 30
      println(s"Streaming started, wait for $awaitTime seconds")
      query.awaitTermination(1000 * awaitTime) // run streaming for specified time range
      query.stop()
      query.awaitTermination(1000 * awaitTime) // wait till query is stopped
    }

    // Step 5. Read final result from delta
    val result = TestSparkKafkaAvroSchemaRegistryUtils.readDataFromDelta(spark, s"${testsCommonDir + testCurrentDir}delta/out/")

    // Step 6. Compare fact result with initial (as we have 1-to-1 transfer)
    assert(
      result.except(in).count == 0 &&
        in.except(result).count == 0 &&
        in.count === result.count,
      "Expected result does not concise with obtained, FAIL!")

    // Step 7. Clean up test state for repeatability
    Directory(new File(s"${testsCommonDir + testCurrentDir}delta/")).deleteRecursively // delete output directory
    SchemaManager.reset // reset Schema Registry to be able to use it across multiple tests
  }
}
