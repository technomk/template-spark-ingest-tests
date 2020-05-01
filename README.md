This is a sample Spark Streaming application that reads data from Kafka topic and writes it into Delta Lake table.

Tags: Kafka, EmbeddedKafka, SchemaRegistry, Confluent, Avro, Spark, SparkStreaming, DeltaLake, SBT, PureConfig, Tests, ScalaTest

#SBT

Hint. To find JAR library by package/object name:
    - import package/object to project (in any file via *import* statement)
    - press Ctrl + left mouse click - IntelliJ will suggest you all available sources for this project/class

Below is the example of how SBT transitive dependencies are resolved. It all started from the below error.
Example of dependency exception:

    An exception or error caused a run to abort: javax.ws.rs.core.Application.getProperties()Ljava/util/Map; 
    java.lang.NoSuchMethodError: javax.ws.rs.core.Application.getProperties()Ljava/util/Map;
    	at org.glassfish.jersey.server.ApplicationHandler.<init>(ApplicationHandler.java:331)
    	at org.glassfish.jersey.servlet.WebComponent.<init>(WebComponent.java:392)
    	at org.glassfish.jersey.servlet.ServletContainer.init(ServletContainer.java:177)
    	at org.glassfish.jersey.servlet.ServletContainer.init(ServletContainer.java:415)
    	at org.eclipse.jetty.servlet.FilterHolder.initialize(FilterHolder.java:133)
    	at org.eclipse.jetty.servlet.ServletHandler.lambda$initialize$0(ServletHandler.java:746)
    	at java.util.Spliterators$ArraySpliterator.forEachRemaining(Spliterators.java:948)
    	at java.util.stream.Streams$ConcatSpliterator.forEachRemaining(Streams.java:742)
    	at java.util.stream.Streams$ConcatSpliterator.forEachRemaining(Streams.java:742)
    	at java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:580)
    	at org.eclipse.jetty.servlet.ServletHandler.initialize(ServletHandler.java:739)
    	at org.eclipse.jetty.servlet.ServletContextHandler.startContext(ServletContextHandler.java:361)
    	at org.eclipse.jetty.server.handler.ContextHandler.doStart(ContextHandler.java:821)
    	at org.eclipse.jetty.servlet.ServletContextHandler.doStart(ServletContextHandler.java:276)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:117)
    	at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:117)
    	at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:110)
    	at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)
    	at org.eclipse.jetty.server.handler.StatisticsHandler.doStart(StatisticsHandler.java:255)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:117)
    	at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:110)
    	at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)
    	at org.eclipse.jetty.server.handler.gzip.GzipHandler.doStart(GzipHandler.java:425)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)
    	at org.eclipse.jetty.server.Server.start(Server.java:407)
    	at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:110)
    	at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)
    	at org.eclipse.jetty.server.Server.doStart(Server.java:371)
    	at io.confluent.rest.ApplicationServer.doStart(ApplicationServer.java:190)
    	at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)
    	at io.confluent.kafka.schemaregistry.RestApp.start(RestApp.java:76)
    	at net.manub.embeddedkafka.schemaregistry.ops.SchemaRegistryOps$class.startSchemaRegistry(SchemaRegistryOps.scala:76)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.startSchemaRegistry(EmbeddedKafka.scala:65)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$class.withRunningServers(EmbeddedKafka.scala:38)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.withRunningServers(EmbeddedKafka.scala:65)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.withRunningServers(EmbeddedKafka.scala:65)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$$anonfun$withRunningKafkaOnFoundPort$1$$anonfun$apply$3.apply(EmbeddedKafka.scala:123)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$$anonfun$withRunningKafkaOnFoundPort$1$$anonfun$apply$3.apply(EmbeddedKafka.scala:122)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$class.withTempDir(EmbeddedKafka.scala:146)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.withTempDir(EmbeddedKafka.scala:65)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$$anonfun$withRunningKafkaOnFoundPort$1.apply(EmbeddedKafka.scala:122)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$$anonfun$withRunningKafkaOnFoundPort$1.apply(EmbeddedKafka.scala:121)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$$anonfun$withRunningZooKeeper$1.apply(EmbeddedKafka.scala:134)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$$anonfun$withRunningZooKeeper$1.apply(EmbeddedKafka.scala:131)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$class.withTempDir(EmbeddedKafka.scala:146)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.withTempDir(EmbeddedKafka.scala:65)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$class.withRunningZooKeeper(EmbeddedKafka.scala:131)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.withRunningZooKeeper(EmbeddedKafka.scala:65)
    	at net.manub.embeddedkafka.EmbeddedKafkaSupport$class.withRunningKafkaOnFoundPort(EmbeddedKafka.scala:121)
    	at net.manub.embeddedkafka.schemaregistry.EmbeddedKafka$.withRunningKafkaOnFoundPort(EmbeddedKafka.scala:65)
    	at org.technomk.projects.data.spark_kafka.TestSparkKafkaAvroSchemaRegistry$$anonfun$1.apply$mcV$sp(TestSparkKafkaAvroSchemaRegistry.scala:30)
    	at org.technomk.projects.data.spark_kafka.TestSparkKafkaAvroSchemaRegistry$$anonfun$1.apply(TestSparkKafkaAvroSchemaRegistry.scala:15)
    	at org.technomk.projects.data.spark_kafka.TestSparkKafkaAvroSchemaRegistry$$anonfun$1.apply(TestSparkKafkaAvroSchemaRegistry.scala:15)
    	at org.scalatest.OutcomeOf$class.outcomeOf(OutcomeOf.scala:85)
    	at org.scalatest.OutcomeOf$.outcomeOf(OutcomeOf.scala:104)
    	at org.scalatest.Transformer.apply(Transformer.scala:22)
    	at org.scalatest.Transformer.apply(Transformer.scala:20)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anon$1.apply(AnyFunSuiteLike.scala:189)
    	at org.scalatest.TestSuite$class.withFixture(TestSuite.scala:196)
    	at org.scalatest.funsuite.AnyFunSuite.withFixture(AnyFunSuite.scala:1562)
    	at org.scalatest.funsuite.AnyFunSuiteLike$class.invokeWithFixture$1(AnyFunSuiteLike.scala:186)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anonfun$runTest$1.apply(AnyFunSuiteLike.scala:199)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anonfun$runTest$1.apply(AnyFunSuiteLike.scala:199)
    	at org.scalatest.SuperEngine.runTestImpl(Engine.scala:306)
    	at org.scalatest.funsuite.AnyFunSuiteLike$class.runTest(AnyFunSuiteLike.scala:199)
    	at org.scalatest.funsuite.AnyFunSuite.runTest(AnyFunSuite.scala:1562)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anonfun$runTests$1.apply(AnyFunSuiteLike.scala:232)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anonfun$runTests$1.apply(AnyFunSuiteLike.scala:232)
    	at org.scalatest.SuperEngine$$anonfun$traverseSubNodes$1$1.apply(Engine.scala:413)
    	at org.scalatest.SuperEngine$$anonfun$traverseSubNodes$1$1.apply(Engine.scala:401)
    	at scala.collection.immutable.List.foreach(List.scala:392)
    	at org.scalatest.SuperEngine.traverseSubNodes$1(Engine.scala:401)
    	at org.scalatest.SuperEngine.org$scalatest$SuperEngine$$runTestsInBranch(Engine.scala:396)
    	at org.scalatest.SuperEngine.runTestsImpl(Engine.scala:475)
    	at org.scalatest.funsuite.AnyFunSuiteLike$class.runTests(AnyFunSuiteLike.scala:232)
    	at org.scalatest.funsuite.AnyFunSuite.runTests(AnyFunSuite.scala:1562)
    	at org.scalatest.Suite$class.run(Suite.scala:1112)
    	at org.scalatest.funsuite.AnyFunSuite.org$scalatest$funsuite$AnyFunSuiteLike$$super$run(AnyFunSuite.scala:1562)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anonfun$run$1.apply(AnyFunSuiteLike.scala:236)
    	at org.scalatest.funsuite.AnyFunSuiteLike$$anonfun$run$1.apply(AnyFunSuiteLike.scala:236)
    	at org.scalatest.SuperEngine.runImpl(Engine.scala:535)
    	at org.scalatest.funsuite.AnyFunSuiteLike$class.run(AnyFunSuiteLike.scala:236)
    	at org.technomk.projects.data.spark_kafka.TestSparkKafkaAvroSchemaRegistry.org$scalatest$BeforeAndAfterAll$$super$run(TestSparkKafkaAvroSchemaRegistry.scala:13)
    	at org.scalatest.BeforeAndAfterAll$class.liftedTree1$1(BeforeAndAfterAll.scala:213)
    	at org.scalatest.BeforeAndAfterAll$class.run(BeforeAndAfterAll.scala:210)
    	at org.technomk.projects.data.spark_kafka.TestSparkKafkaAvroSchemaRegistry.run(TestSparkKafkaAvroSchemaRegistry.scala:13)
    	at org.scalatest.tools.SuiteRunner.run(SuiteRunner.scala:45)
    	at org.scalatest.tools.Runner$$anonfun$doRunRunRunDaDoRunRun$1.apply(Runner.scala:1320)
    	at org.scalatest.tools.Runner$$anonfun$doRunRunRunDaDoRunRun$1.apply(Runner.scala:1314)
    	at scala.collection.immutable.List.foreach(List.scala:392)
    	at org.scalatest.tools.Runner$.doRunRunRunDaDoRunRun(Runner.scala:1314)
    	at org.scalatest.tools.Runner$$anonfun$runOptionallyWithPassFailReporter$2.apply(Runner.scala:972)
    	at org.scalatest.tools.Runner$$anonfun$runOptionallyWithPassFailReporter$2.apply(Runner.scala:971)
    	at org.scalatest.tools.Runner$.withClassLoaderAndDispatchReporter(Runner.scala:1480)
    	at org.scalatest.tools.Runner$.runOptionallyWithPassFailReporter(Runner.scala:971)
    	at org.scalatest.tools.Runner$.run(Runner.scala:798)
    	at org.scalatest.tools.Runner.run(Runner.scala)
    	at org.jetbrains.plugins.scala.testingSupport.scalaTest.ScalaTestRunner.runScalaTest2(ScalaTestRunner.java:133)
    	at org.jetbrains.plugins.scala.testingSupport.scalaTest.ScalaTestRunner.main(ScalaTestRunner.java:27)

It says that EmbeddedKafka starts Confluent Schema Registry, which, in turn, starts internal web server via `org.glassfish.jersey.server`.
At some moment, *glassfish* calls `Application.getProperties()` method (`Application` class is located in `javax.ws.rs.core` package).

This package and class is added to project by two libraries: `com.sun.jersey:jersey-core:1.9:jar` (this is an old implementation of `org.glassfish.jersey`) and `javax.ws.rs:javax.ws.rs-api:2.0.1:jar`.
The first one of them doesn't have `getProperties()` method, while the second one has. So we need to get rid of old version of package, which is loaded from `com.sun.jersey:jersey-core:1.9:jar`.

For this reason, we need to exclude this dependency from where it taken of.
Using `addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")` SBT plugin (specifically with the help of `dependencyBrowseGraph` command) I've created below dependency graph.
There are some inline comments (in blue) of what should be done.







