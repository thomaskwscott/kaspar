package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.predicate.{MinMaxPredicateGenerator, PredicateType}
import kaspar.dataload.structure.JsonValueRowDeserializer
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import scala.sys.process._

class SegmentPredicatesIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "apply segment predicates" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object SegmentPredicatesIntegrationTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("SegmentPredicatesIntegrationTest")
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

    val kasparDriver = KasparDriverBuilder()
      .withClientProperties(clientProps)
      .build()

    val customerRawRows = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer)

    // 10 rows
    try {
      val rowCount = customerRawRows.collect.size
      if (rowCount != 10) throw new AssertionError("expected 10 rows but got " + rowCount)
    } catch {
      case e => TestRunner.fail(e)
    }

    // we don't index the active segment and all segments currently associated with Customers_json are active.
    // We need to add some more dummy records to force a segment roll
    // this will add 6 dummy customers (1 per partition) to force a segment roll in each Customers_json partition
    // because these new dummy records will be in the new active segment they will always be returned in our
    // queries and we must filter them out to show the segment predicates
    Seq("sh",
      "-c",
      "/home/ubuntu/add_dummy_customers.sh") ! ProcessLogger(stdout append _, stderr append _)

    // let's create the index again:
    val minMaxPredicateGenerator = new MinMaxPredicateGenerator()

    val newIndexReport = kasparDriver.createIndex(sc, "Customers_json", customersRowDeserializer,
      minMaxPredicateGenerator.getIndexName(),minMaxPredicateGenerator.getIndexFunction(Seq((6,ColumnType.INTEGER))))

    // this shows >0
    try {
      val newIndexCount = newIndexReport.collect.size
      if (newIndexCount <= 0) throw new AssertionError("expected to create >0 index files but created: " + newIndexCount)
    } catch {
      case e => TestRunner.fail(e)
    }

    // segment predicate example, this uses the previously created index files to read only segments that contain customers
    // aged above 80
    val minMax80Predicate = minMaxPredicateGenerator.segmentPredicateFromJson(
      """
        |{
        |  "predicateType": "GreaterThan",
        |  "threshold" : 80,
        |  "columnIndex" : 6
        |}
        |""".stripMargin
    )
    val customerSegmentPredicateRawRowsHigh = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer,
      segmentPredicates = Array(minMax80Predicate))

    // this should show 0 rows as the segments contain no rows with that value (filtering out dummy records)
    try {
      val greaterThan80Count = customerSegmentPredicateRawRowsHigh.collect.filter(_.getStringVal(4) != "Dummy").size
      if (greaterThan80Count != 0) throw new AssertionError("expected greater than 80 count to be 0 but got: " + greaterThan80Count)
    } catch {
      case e => TestRunner.fail(e)
    }

    // We know that we have one row with an age higher than 45, the segment
    // predicate will read all segments that contain this rows (and all rows in the segment). Depending on the way the
    // messages are partitioned in kafka this could return 1 or more rows.
    // aged above 45
    val minMax45Predicate = minMaxPredicateGenerator.segmentPredicateFromJson(
      """
        |{
        |  "predicateType": "GreaterThan",
        |  "threshold" : 45,
        |  "columnIndex" : 6
        |}
        |""".stripMargin
    )
    val customerSegmentPredicateRawRowsLow = kasparDriver.getRows(sc,
      "Customers_json",
      customersRowDeserializer,
      segmentPredicates = Array(minMax45Predicate))

    // this should show >0 rows as there is at least 1 segment containing a row with age >45 (filtering out dummy records)
    try {
      val greaterThan45Count = customerSegmentPredicateRawRowsLow.collect.filter(_.getStringVal(4) != "Dummy").size
      if (greaterThan45Count <= 0 ) throw new AssertionError("expected greater than 45 count to be >0 but got: " + greaterThan45Count)
    } catch {
      case e => TestRunner.fail(e)
    }

    TestRunner.recordResult("Segment predicate test completed")
  }
}
