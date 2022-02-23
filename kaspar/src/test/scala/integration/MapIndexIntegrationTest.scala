package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.predicate.{MapIndexPredicateGenerator, MinMaxPredicateGenerator, PredicateType}
import kaspar.dataload.structure.JsonValueRowDeserializer
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class MapIndexIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "use map indexes" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object MapIndexIntegrationTest {
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

    val kasparDriver = KasparDriverBuilder().withClientProperties(clientProps).build()

    val customerRawRows = kasparDriver.getRows(sc, "Customers_index", customersRowDeserializer)

    // 4 rows
    try {
      val rowCount = customerRawRows.collect.size
      if (rowCount != 4) throw new AssertionError("expected 4 rows but got " + rowCount)
    } catch {
      case e => TestRunner.fail(e)
    }

    // let's create the index:
    val mapIndexGenerator = new MapIndexPredicateGenerator()

    val newIndexReport = kasparDriver.createIndex(sc, "Customers_index", customersRowDeserializer,
      mapIndexGenerator.getIndexName(),mapIndexGenerator.getIndexFunction(Seq((4,ColumnType.STRING))))

    // this shows >0
    try {
      val newIndexCount = newIndexReport.collect.size
      if (newIndexCount <= 0) throw new AssertionError("expected to create >0 index files but created: " + newIndexCount)
    } catch {
      case e => TestRunner.fail(e)
    }

    // segment predicate example, this fetches batman!
    val waynePredicate = mapIndexGenerator.segmentPredicateFromJson(
      """
        |{
        |  "value": "Bruce Wayne",
        |  "columnIndex" : 4
        |}
        |""".stripMargin
    )
    val wayneCustomers = kasparDriver.getRows(sc, "Customers_index", customersRowDeserializer,
      segmentPredicates = Array(waynePredicate))

    // this should show 1 rows after filtering the active segment
    try {
      val wayneCount = wayneCustomers.collect.filter(_.getStringVal(4) != "Dummy2").size
      if (wayneCount != 1) throw new AssertionError("expected Bruce Wayne count to be 1 but got: " + wayneCount)
    } catch {
      case e => TestRunner.fail(e)
    }

    TestRunner.recordResult("Map Index predicate test completed")
  }
}
