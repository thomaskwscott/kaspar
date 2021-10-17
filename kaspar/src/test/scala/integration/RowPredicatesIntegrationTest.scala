package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.structure.{JsonValueRowDeserializer, RawRow}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class RowPredicatesIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "apply row predicates" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object RowPredicatesIntegrationTest {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder
      .appName("PathJsonSelectIntegrationTest")
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

    // no predicates
    val customerRawRows = kasparDriver.getRows(sc , "Customers_json", customersRowDeserializer)


    // row predicate example, this fetches all customers with a name that start with 'B'
    val customerRowPredicateRawRows = kasparDriver.getRows(sc, "Customers_json", customersRowDeserializer,
      rowPredicates = Array((rawRow: RawRow) => rawRow.getColumnVal(4).toString().startsWith("B")))

    val predicateRows = customerRowPredicateRawRows.collect
    TestRunner.recordResult(predicateRows.map(_.toString()).mkString("\n"));

    try {
      val allRowCount = customerRawRows.collect.size
      val predicateRowCount = predicateRows.size
      if (allRowCount != 10) throw new AssertionError("expected 10 rows with no predicate but got " + allRowCount)
      if (predicateRowCount != 2) throw new AssertionError("expected 2 rows with predicate but got " + allRowCount)
    } catch {
      case e => TestRunner.fail(e)
    }
  }
}
