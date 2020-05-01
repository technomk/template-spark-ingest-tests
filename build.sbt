organization := "org.technomk.projects.data.templates"
name := "template-spark-ingest-tests"
version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.4.3"

// === dependencies required for the project (limited by the provided scope if any, e.g. Test, Provided,  Compile, etc.)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion, // connection to Kafka (added kafka format for read/write)
  "org.apache.spark" %% "spark-avro" % sparkVersion, // used by other libraries to work with avro

  // https://mvnrepository.com/artifact/za.co.absa/abris
  "za.co.absa" %% "abris" % "3.1.0" excludeAll // provides seamless integration between Avro and Spark Structured APIs
      ExclusionRule(organization = "org.apache.spark"), // abris version 3.1.0 has Spark 2.4.4 inside, so exclude it

  // https://mvnrepository.com/artifact/com.github.pureconfig/pureconfig
  "com.github.pureconfig" %% "pureconfig" % "0.12.3", // library for loading configuration files

  // https://mvnrepository.com/artifact/io.delta/delta-core
  "io.delta" %% "delta-core" % "0.6.0", // adds Delta Lake table sink and source

  // https://mvnrepository.com/artifact/io.github.embeddedkafka/embedded-kafka-schema-registry
  "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % "5.4.0" % Test excludeAll( // embedded Kafka with Schema Registry
    ExclusionRule(organization = "org.glassfish.jersey.containers"), // embeddedkafka has org.glassfish.jersey version 2.28, while Spark has version 2.22.2 inside it
    ExclusionRule(organization = "org.glassfish.jersey.core"),
    ExclusionRule(organization = "org.glassfish.jersey.ext") // this dependency is not used in Spark, but anyway we need consistent version, so add it below separately
  ),
  "org.glassfish.jersey.ext" % "jersey-bean-validation" % "2.22.2" % Test, // must be in sync with jersey dependencies versions of Spark

  // https://mvnrepository.com/artifact/org.scalatest/scalatest
  "org.scalatest" %% "scalatest" % "3.1.1", // % Test, // core scala testing framework

  // https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.14.0" % Test // additional methods and objects for testing (e.g. spark session)
)

// === resolvers to take libraries from (maven repository is default)
resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io/maven/" // e.g. required for abris to get io.confluent:<XXX>:5.1.0 (version 5.1.0 not available in Maven)
)

// === explicit overriding of dependencies, used to explicitly set desired version of library (by default, max version is chosen)
dependencyOverrides ++= Seq(
  "io.confluent" % "kafka-schema-registry" % "5.4.0", // use the higher version of confluent in abris
  "org.apache.hadoop" % "hadoop-client" % "2.6.5", // holdenkarau has Hadoop version 2.8.3, so better not override out 2.6.5 version inherited from Spark

  "com.fasterxml.jackson.core" % "jackson-core" % "2.7.2", // used in Spark Streaming micro-batch execution
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.7.2"
)

// globally excluded dependencies
excludeDependencies ++= Seq(
  ExclusionRule(organization = "com.sun.jersey"), // used in Hadoop, exclude old jersey implementation from the project
  ExclusionRule(organization = "org.mortbay.jetty"), // used in Hadoop, but newer version in javax.servlet:javax.servlet-api
)