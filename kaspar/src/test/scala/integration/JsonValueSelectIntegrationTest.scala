package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.structure.JsonValueRowDeserializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class JsonValueSelectIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "run simple select statements in JSON data" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object JsonValueSelectIntegrationTest {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("JsonValueSelectIntegrationTest")
      .getOrCreate()
    val sc  = spark.sparkContext

    val clientProps = new java.util.Properties
    clientProps.setProperty("bootstrap.servers","worker1:9091")

    val customersColumnMappings =   Array(
      ("id",ColumnType.INTEGER),
      ("name",ColumnType.STRING),
      ("address",ColumnType.STRING),
      ("age",ColumnType.INTEGER)
    )
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

    val sqlContext = new SQLContext(sc)

    kasparDriver.registerTable(sqlContext, customerRawRows,"Customers",customersColumnMappings)

    val customersSql =
      """
        | select *
        | from Customers
        |""".stripMargin

    val resultDf = sqlContext.sql(customersSql)
    val collected = resultDf.collect()

    TestRunner.recordResult(collected.map(_.toString()).mkString("\n"));

    try {
      val rowCount = collected.length
      if (rowCount != 10) throw new AssertionError("expected 10 rows but got " + rowCount)
    } catch {
      case e => TestRunner.fail(e)
    }

  }
}
