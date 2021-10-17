package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.predicate.OffsetPredicateGenerator
import kaspar.dataload.structure.JsonValueRowDeserializer
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import scala.sys.process._

class TopUpIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "allow top up of row data" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object TopUpIntegrationTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("TopUpIntegrationTest")
      .getOrCreate()
    val sc = spark.sparkContext
    val clientProps = new java.util.Properties
    clientProps.setProperty("bootstrap.servers", "worker1:9091")

    val customersRowDeserializer = new JsonValueRowDeserializer()
    val customersRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "id",
        |       "type" : "Integer"
        |     },
        |     {
        |       "name" : "name",
        |       "type" : "String"
        |     },
        |     {
        |       "name" : "address",
        |       "type" : "String"
        |     },
        |     {
        |       "name" : "age",
        |       "type" : "Integer"
        |     }
        |  ]
        |}
        |""".stripMargin
    customersRowDeserializer.configure(customersRowDeserializerConfig)

    val initialThresholds = Map(
      0 -> -1L
    )

    val kasparDriver = KasparDriverBuilder()
      .withClientProperties(clientProps)
      .build()

    val offsetPredicateGenerator = new OffsetPredicateGenerator()
    val initialOffsetPredicate = offsetPredicateGenerator.rowPredicateFromJson(
      """
        |{
        |  "predicateType": "GreaterThan",
        |  "partitionThresholds" : [
        |    { "partition": 0, "threshold": -1 }
        |  ]
        |}
        |""".stripMargin
    )
    val customerRawRows = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer,
      rowPredicates = Array(initialOffsetPredicate))

    val persistedRows = customerRawRows.persist()

    // 10 rows
    try {
      val rowCount = persistedRows.collect().size
      if (rowCount != 10) throw new AssertionError("expected 10 rows but got " + rowCount)
    } catch {
      case e => TestRunner.fail(e)
    }
    // lets get the latest offset we have read for each partition in a map (this will be used as input to the next predicate)
    val latestThresholds = (persistedRows.collect.map(i=>(i.getIntVal(1).asInstanceOf[Int],i.getLongVal(0).asInstanceOf[Long])).toSet ++ initialThresholds.toSet).groupBy(_._1).mapValues(i => i.map(_._2).max).map(identity)

    // let's add some more customers
    Thread.sleep(30000)
    Seq("sh",
      "-c",
      "/home/ubuntu/add_dummy_customers.sh") ! ProcessLogger(stdout append _, stderr append _)

    // now lets top up
    val latestThresholdsJson1 = latestThresholds.map({case(partition, threshold) =>
      s"""
         | { "partition": $partition, "threshold": $threshold }
         |""".stripMargin}).mkString(",")
    val latestThresholdPredicate1 = offsetPredicateGenerator.rowPredicateFromJson(
      s"""
        |{
        |  "predicateType": "GreaterThan",
        |  "partitionThresholds" : [
        |  $latestThresholdsJson1
        |  ]
        |}
        |""".stripMargin
    )
    val topUpRowRawRows = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer,
      rowPredicates = Array(latestThresholdPredicate1))

    // this should fetch only 6 rows each with offsets higher than those in the latestThresholds map
    try {
      val topUp = topUpRowRawRows.collect
      if (topUp.size != 6) throw new AssertionError("expected 6 rows but got " + topUp.size)
      val lowestOffset = topUp.map(i => i.getLongVal(0)).min
      if (lowestOffset != 10) throw new AssertionError("expected lowest offset to be 10 but got " + lowestOffset)
    } catch {
      case e => TestRunner.fail(e)
    }

    // now lets top up
    val latestThresholdsJson2 = latestThresholds.map({case(partition, threshold) =>
      s"""
         | { "partition": $partition, "threshold": $threshold }
         |""".stripMargin}).mkString(",")
    val latestThresholdPredicate2 = offsetPredicateGenerator.segmentPredicateFromJson(
      s"""
         |{
         |  "predicateType": "GreaterThan",
         |  "partitionThresholds" : [
         |  $latestThresholdsJson2
         |  ]
         |}
         |""".stripMargin
    )

    // as the new message were written to a new segment (we have a very low segment.ms on this topic). We should be able to achieve the same result with a segment predicate
    val topUpSegmentRawRows = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer,
      segmentPredicates = Array(latestThresholdPredicate2))

    // this should fetch the same 2 rows:
    try {
      val topUp = topUpSegmentRawRows.collect
      if (topUp.size != 6) throw new AssertionError("expected 6 rows but got " + topUp.size)
      val lowestOffset = topUp.map(i => i.getLongVal(0)).min
      if (lowestOffset != 10) throw new AssertionError("expected lowest offset to be 10 but got " + lowestOffset)
    } catch {
      case e => TestRunner.fail(e)
    }

    // now lets top up
    val latestThresholdsJson3 = latestThresholds.map({case(partition, threshold) =>
      s"""
         | { "partition": $partition, "threshold": $threshold }
         |""".stripMargin}).mkString(",")
    val latestThresholdRowPredicate3 = offsetPredicateGenerator.rowPredicateFromJson(
      s"""
         |{
         |  "predicateType": "GreaterThan",
         |  "partitionThresholds" : [
         |  $latestThresholdsJson3
         |  ]
         |}
         |""".stripMargin
    )
    val latestThresholdSegmentPredicate3 = offsetPredicateGenerator.segmentPredicateFromJson(
      s"""
         |{
         |  "predicateType": "GreaterThan",
         |  "partitionThresholds" : [
         |  $latestThresholdsJson3
         |  ]
         |}
         |""".stripMargin
    )

    // row and segment predicates can (and should) be used in conjunction with eachother
    val topUpRawRows = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer,
      rowPredicates = Array(latestThresholdRowPredicate3),
      segmentPredicates = Array(latestThresholdSegmentPredicate3))

    // this should fetch only 2 rows each with offsets higher than those in the latestThresholds map
    try {
      val topUp = topUpRawRows.collect
      if (topUp.size != 6) throw new AssertionError("expected 6 rows but got " + topUp.size)
      val lowestOffset = topUp.map(i => i.getLongVal(0)).min
      if (lowestOffset != 10) throw new AssertionError("expected lowest offset to be 10 but got " + lowestOffset)
    } catch {
      case e => TestRunner.fail(e)
    }

    // 16 rows now
    try {
      val allRows = sc.union(persistedRows, topUpRawRows)
      val allRowsSize = allRows.collect().size
      if (allRowsSize != 16) throw new AssertionError("expected 16 rows but got " + allRowsSize)
      TestRunner.recordResult(allRows.collect().map(_.toString()).mkString("\n"));
    } catch {
      case e => TestRunner.fail(e)
    }
  }
}

