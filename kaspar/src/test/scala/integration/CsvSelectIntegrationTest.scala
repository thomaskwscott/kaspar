package integration

import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.structure.CsvRowDeserializer
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class CsvSelectIntegrationTest extends AnyFlatSpec {

  "A Kaspar cluster" should "run complex select statements on CSV data" taggedAs (DOCKER) in {
    TestRunner.run(this.getClass.getName)
  }
}

object CsvSelectIntegrationTest {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .appName("CsvSelectIntegrationTest")
      .getOrCreate()
    val sc  = spark.sparkContext

    val clientProps = new java.util.Properties
    clientProps.setProperty("bootstrap.servers","worker1:9091")

    val csvRowDeserializer = new CsvRowDeserializer()
    val csvRowDeserializerConfig = """
      |{
      |  "delimiter" : ","
      |}
      |""".stripMargin

    csvRowDeserializer.configure(csvRowDeserializerConfig)

    val kasparDriver = KasparDriverBuilder()
      .withClientProperties(clientProps)
      .build()

    val customerRawRows = kasparDriver.getRows(sc, "Customers", csvRowDeserializer)

    val customerRows = customerRawRows.map(rawRow => RowFactory.create(
      rawRow.getColumnVal(0).toString(),
      rawRow.getColumnVal(1).toString(),
      rawRow.getColumnVal(2).toString(),
      Integer.valueOf(rawRow.getColumnVal(3).toString()),
      rawRow.getColumnVal(4).toString(),
      rawRow.getColumnVal(5).toString(),
      Integer.valueOf(rawRow.getColumnVal(6).toString())
    ))

    val customerCols = Array(
      StructField("offset", DataTypes.StringType, false, Metadata.empty),
      StructField("partition", DataTypes.StringType, false, Metadata.empty),
      StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
      StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
      StructField("name", DataTypes.StringType, false, Metadata.empty),
      StructField("address", DataTypes.StringType, false, Metadata.empty),
      StructField("age", DataTypes.IntegerType, false, Metadata.empty)
    )
    val customerSchema = new StructType(customerCols)

    val transactionRawRows = kasparDriver.getRows(sc, "Transactions", csvRowDeserializer)

    val transactionRows = transactionRawRows.map(rawRow => RowFactory.create(
      rawRow.getColumnVal(0).toString(),
      rawRow.getColumnVal(1).toString(),
      rawRow.getColumnVal(2).toString(),
      Integer.valueOf(rawRow.getColumnVal(3).toString()),
      Integer.valueOf(rawRow.getColumnVal(4).toString())
    ))

    val transactionCols = Array(
      StructField("offset", DataTypes.StringType, false, Metadata.empty),
      StructField("partition", DataTypes.StringType, false, Metadata.empty),
      StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
      StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
      StructField("itemId", DataTypes.IntegerType, false, Metadata.empty)
    )
    val transactionSchema = new StructType(transactionCols)

    val itemRawRows = kasparDriver.getRows(sc, "Items", csvRowDeserializer)

    val itemRows = itemRawRows.map(rawRow =>   RowFactory.create(
      rawRow.getColumnVal(0).toString(),
      rawRow.getColumnVal(1).toString(),
      rawRow.getColumnVal(2).toString(),
      Integer.valueOf(rawRow.getColumnVal(3).toString()),
      rawRow.getColumnVal(4).toString(),
      java.lang.Double.valueOf(rawRow.getColumnVal(5).toString())
    ))

    val itemCols = Array(
      StructField("offset", DataTypes.StringType, false, Metadata.empty),
      StructField("partition", DataTypes.StringType, false, Metadata.empty),
      StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
      StructField("itemId", DataTypes.IntegerType, false, Metadata.empty),
      StructField("description", DataTypes.StringType, false, Metadata.empty),
      StructField("price", DataTypes.DoubleType, false, Metadata.empty)
    )
    val itemSchema = new StructType(itemCols)

    // run some sql
    val sqlContext = new SQLContext(sc);

    val customerDf = sqlContext.createDataFrame(customerRows,customerSchema)
    customerDf.createOrReplaceTempView("Customers")

    val transactionDf = sqlContext.createDataFrame(transactionRows,transactionSchema)
    transactionDf.createOrReplaceTempView("Transactions")

    val itemDf = sqlContext.createDataFrame(itemRows,itemSchema)
    itemDf.createOrReplaceTempView("Items")

    val sql =
      """
        | select name,price
        | from Customers
        | join Transactions
        | on Customers.customerId = Transactions.customerId
        | join Items
        | on Items.itemId = Transactions.itemId
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
