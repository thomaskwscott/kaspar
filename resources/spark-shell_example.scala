import collection.JavaConverters._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.CsvColumnifier
import kaspar.dataload.structure.RawRow

val clientProps = new java.util.Properties
clientProps.setProperty("bootstrap.servers","worker1:9091")
val csvColumnifier = new CsvColumnifier(",")

val dataDir = "/var/lib/kafka/data"
val serverProperties = "/etc/kafka/kafka.properties"

val customerRawRows = TopicLoader.getRawRows(sc,dataDir,serverProperties,"Customers",clientProps,csvColumnifier,
  (rawRow: RawRow) => rawRow.getColumnVal(3).startsWith("B"))
customerRawRows.persist

val customerRows = customerRawRows.map(rawRow => RowFactory.create(
  rawRow.getColumnVal(0),
  rawRow.getColumnVal(1),
  Integer.valueOf(rawRow.getColumnVal(2)),
  rawRow.getColumnVal(3),
  rawRow.getColumnVal(4),
  Integer.valueOf(rawRow.getColumnVal(5))
))

val customerCols = Array(
  new StructField("offset", DataTypes.StringType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("name", DataTypes.StringType, false, Metadata.empty),
  new StructField("address", DataTypes.StringType, false, Metadata.empty),
  new StructField("age", DataTypes.IntegerType, false, Metadata.empty)
)
val customerSchema = new StructType(customerCols)


val transactionRawRows = TopicLoader.getRawRows(sc,dataDir,serverProperties,"Transactions",clientProps,csvColumnifier)
transactionRawRows.persist

val transactionRows = transactionRawRows.map(rawRow => RowFactory.create(
  rawRow.getColumnVal(0),
  rawRow.getColumnVal(1),
  Integer.valueOf(rawRow.getColumnVal(2)),
  Integer.valueOf(rawRow.getColumnVal(3))
))

val transactionCols = Array(
  new StructField("offset", DataTypes.StringType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty)
)
val transactionSchema = new StructType(transactionCols)

val itemRawRows = TopicLoader.getRawRows(sc,dataDir,serverProperties,"Items",clientProps,csvColumnifier)
itemRawRows.persist

val itemRows = itemRawRows.map(rawRow =>   RowFactory.create(
  rawRow.getColumnVal(0),
  rawRow.getColumnVal(1),
  Integer.valueOf(rawRow.getColumnVal(2)),
  rawRow.getColumnVal(3),
  java.lang.Double.valueOf(rawRow.getColumnVal(4))
))

val itemCols = Array(
  new StructField("offset", DataTypes.StringType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("description", DataTypes.StringType, false, Metadata.empty),
  new StructField("price", DataTypes.DoubleType, false, Metadata.empty)
)
val itemSchema = new StructType(itemCols)

// run some sql

val sqlContext = new SQLContext(sc)

val transactionDf = sqlContext.createDataFrame(transactionRows,transactionSchema)
transactionDf.createOrReplaceTempView("Transactions")

val customerDf = sqlContext.createDataFrame(customerRows,customerSchema)
customerDf.createOrReplaceTempView("Customers")

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

sqlContext.sql(sql).show(100)