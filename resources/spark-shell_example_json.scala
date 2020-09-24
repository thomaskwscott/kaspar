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
import kaspar.dataload.structure.JsonColumnifier
import kaspar.dataload.structure.RawRow
import kaspar.dataload.predicate.MinMaxPredicate
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType

val clientProps = new java.util.Properties
clientProps.setProperty("bootstrap.servers","worker1:9091")
val customersColumnifier = new JsonColumnifier(
  Array(
    ("id",ColumnType.INTEGER),
    ("name",ColumnType.STRING),
    ("address",ColumnType.STRING),
    ("age",ColumnType.INTEGER)
  )
)

val itemsColumnifier = new JsonColumnifier(
  Array(
    ("id",ColumnType.INTEGER),
    ("name",ColumnType.STRING),
    ("price",ColumnType.DOUBLE)
  )
)

val transactionsColumnifier = new JsonColumnifier(
  Array(
    ("customer_id",ColumnType.INTEGER),
    ("item_id",ColumnType.INTEGER),
  )
)


val customerRawRows = TopicLoader.getRawRows(sc,"Customers_json",clientProps,customersColumnifier,
  rowPredicates = Array((rawRow: RawRow) => rawRow.getColumnVal(3).toString().startsWith("B")))

customerRawRows.persist

val customerRows = customerRawRows.map(rawRow => RowFactory.create(
  rawRow.getLongVal(0),
  rawRow.getLongVal(1),
  rawRow.getIntVal(2),
  rawRow.getStringVal(3),
  rawRow.getStringVal(4),
  rawRow.getIntVal(5)
))

val customerCols = Array(
  new StructField("offset", DataTypes.LongType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.LongType, false, Metadata.empty),
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("name", DataTypes.StringType, false, Metadata.empty),
  new StructField("address", DataTypes.StringType, false, Metadata.empty),
  new StructField("age", DataTypes.IntegerType, false, Metadata.empty)
)
val customerSchema = new StructType(customerCols)


val transactionRawRows = TopicLoader.getRawRows(sc,"Transactions_json",clientProps,transactionsColumnifier)
transactionRawRows.persist

val transactionRows = transactionRawRows.map(rawRow => RowFactory.create(
  rawRow.getLongVal(0),
  rawRow.getLongVal(1),
  rawRow.getIntVal(2),
  rawRow.getIntVal(3)
))

val transactionCols = Array(
  new StructField("offset", DataTypes.LongType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.LongType, false, Metadata.empty),
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty)
)
val transactionSchema = new StructType(transactionCols)

val itemRawRows = TopicLoader.getRawRows(sc,"Items_json",clientProps,itemsColumnifier)
itemRawRows.persist

val itemRows = itemRawRows.map(rawRow =>   RowFactory.create(
  rawRow.getLongVal(0),
  rawRow.getLongVal(1),
  rawRow.getIntVal(2),
  rawRow.getStringVal(3),
  rawRow.getDoubleVal(4)
))

val itemCols = Array(
  new StructField("offset", DataTypes.LongType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.LongType, false, Metadata.empty),
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("description", DataTypes.StringType, false, Metadata.empty),
  new StructField("price", DataTypes.DoubleType, false, Metadata.empty)
)
val itemSchema = new StructType(itemCols)

// run some sql
val sqlContext = new SQLContext(sc)

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

sqlContext.sql(sql).show(100)