package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.structure.JsonKeyValueRowDeserializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class JsonKeyValueSelectIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "run simple select statements in JSON keyed data" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}


object JsonKeyValueSelectIntegrationTest {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("PathJsonSelectIntegrationTest")
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
    val customersDeserializer = new JsonKeyValueRowDeserializer()
    val customersRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "id",
        |       "type" : "Integer",
        |       "isKey" : false
        |     },
        |     {
        |       "name" : "name",
        |       "type" : "String",
        |       "isKey" : false
        |     },
        |     {
        |       "name" : "address",
        |       "type" : "String",
        |       "isKey" : false
        |     },
        |     {
        |       "name" : "age",
        |       "type" : "Integer",
        |       "isKey" : false
        |     }
        |  ]
        |}
        |""".stripMargin
    customersDeserializer.configure(customersRowDeserializerConfig)

    val itemsColumnMappings = Array(
      ("item_id",ColumnType.INTEGER),
      ("name",ColumnType.STRING),
      ("price",ColumnType.DOUBLE)
    )
    val itemsDeserializer = new JsonKeyValueRowDeserializer()
    val itemsRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "item_id",
        |       "type" : "Integer",
        |       "isKey" : true
        |     },
        |     {
        |       "name" : "name",
        |       "type" : "String",
        |       "isKey" : false
        |     },
        |     {
        |       "name" : "price",
        |       "type" : "Double",
        |       "isKey" : false
        |     }
        |  ]
        |}
        |""".stripMargin
    itemsDeserializer.configure(itemsRowDeserializerConfig)

    val transactionsColumnMappings = Array(
      ("customer_id",ColumnType.INTEGER),
      ("item_id",ColumnType.INTEGER),
    )
    val transactionsDeserializer = new JsonKeyValueRowDeserializer()
    val transactionsRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "customer_id",
        |       "type" : "Integer",
        |       "isKey" : false
        |     },
        |     {
        |       "name" : "item_id",
        |       "type" : "Integer",
        |       "isKey" : false
        |     }
        |  ]
        |}
        |""".stripMargin
    transactionsDeserializer.configure(transactionsRowDeserializerConfig)

    val kasparDriver = KasparDriverBuilder()
      .withClientProperties(clientProps)
      .build()

    val customerRawRows = kasparDriver.getRows(sc, "Customers_json", customersDeserializer)

    val transactionRawRows = kasparDriver.getRows(sc, "Transactions_json", transactionsDeserializer)

    val itemRawRows = kasparDriver.getRows(sc, "Items_json", itemsDeserializer)

    val sqlContext = new SQLContext(sc)

    kasparDriver.registerTable(sqlContext, customerRawRows, "Customers", customersColumnMappings)
    kasparDriver.registerTable(sqlContext, transactionRawRows, "Transactions", transactionsColumnMappings)
    kasparDriver.registerTable(sqlContext, itemRawRows, "Items", itemsColumnMappings)

    val sql =
      """
        | select Customers.name,Items.name,price
        | from Customers
        | join Transactions
        | on Customers.id = Transactions.customer_id
        | join Items
        | on Items.item_id = Transactions.item_id
        |""".stripMargin

    val resultDf = sqlContext.sql(sql)
    val collected = resultDf.collect()

    TestRunner.recordResult(collected.map(_.toString()).mkString("\n"));

    try {
      val rowCount = collected.length
      if (rowCount != 100) throw new AssertionError("expected 100 rows but got " + rowCount)
    } catch {
      case e => TestRunner.fail(e)
    }

  }
}
