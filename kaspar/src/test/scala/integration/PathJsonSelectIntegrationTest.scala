package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.structure.PathJsonValueRowDeserializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class PathJsonSelectIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "run simple select statements in JSON path data" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object PathJsonSelectIntegrationTest {
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
    val customersDeserializer = new PathJsonValueRowDeserializer()
    val customersRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "id",
        |       "type" : "Integer",
        |       "path" : "$.id"
        |     },
        |     {
        |       "name" : "name",
        |       "type" : "String",
        |       "path" : "$.name"
        |     },
        |     {
        |       "name" : "address",
        |       "type" : "String",
        |       "path" : "$.address"
        |     },
        |     {
        |       "name" : "age",
        |       "type" : "Integer",
        |       "path" : "$.age"
        |     }
        |  ]
        |}
        |""".stripMargin
    customersDeserializer.configure(customersRowDeserializerConfig)

    val itemsColumnMappings = Array(
      ("id",ColumnType.INTEGER),
      ("name",ColumnType.STRING),
      ("price",ColumnType.DOUBLE),
      ("margin",ColumnType.DOUBLE)
    )
    val itemsDeserializer = new PathJsonValueRowDeserializer()
    val itemsRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "id",
        |       "type" : "Integer",
        |       "path" : "$.id"
        |     },
        |     {
        |       "name" : "name",
        |       "type" : "String",
        |       "path" : "$.name"
        |     },
        |     {
        |       "name" : "price",
        |       "type" : "Double",
        |       "path" : "$.price"
        |     },
        |     {
        |       "name" : "margin",
        |       "type" : "Double",
        |       "path" : "$.internal.margin"
        |     }
        |  ]
        |}
        |""".stripMargin
    itemsDeserializer.configure(itemsRowDeserializerConfig)

    val transactionsColumnMappings = Array(
      ("customer_id",ColumnType.INTEGER),
      ("item_id",ColumnType.INTEGER),
    )
    val transactionsDeserializer = new PathJsonValueRowDeserializer()
    val transactionsRowDeserializerConfig =
      """
        |{
        |  "columns" : [
        |    {
        |       "name" : "customer_id",
        |       "type" : "Integer",
        |       "path" : "$.customer_id"
        |     },
        |     {
        |       "name" : "item_id",
        |       "type" : "Integer",
        |       "path" : "$.item_id"
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
    kasparDriver.registerTable(sqlContext, itemRawRows, "Items", itemsColumnMappings.map(i => (i._1,i._2)))

    val sql =
      """
        | select Customers.name,Items.name,price,margin
        | from Customers
        | join Transactions
        | on Customers.id = Transactions.customer_id
        | join Items
        | on Items.id = Transactions.item_id
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
