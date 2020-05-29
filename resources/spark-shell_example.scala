
import collection.JavaConverters._
import uk.co.threefi.dataload.SegmentLoader
import uk.co.threefi.dataload.structure.Columnifier
import uk.co.threefi.dataload.structure.CsvColumnifier
import uk.co.threefi.dataload.structure.RawRow
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.function.Function;

val clientProps = new java.util.Properties
clientProps.setProperty("bootstrap.servers","worker1:9091")
val csvColumnifier = new CsvColumnifier(",");

val customerRawRows = SegmentLoader.getRawRows(sc,"Customers",clientProps,csvColumnifier);
val customerConversionFunc = new org.apache.spark.api.java.function.Function[RawRow, Row] {
  final val serialVersionUID = -812004521983071103L
  override def call(rawRow: RawRow) : Row = RowFactory.create(
    Integer.valueOf(rawRow.getColumnVal(0)),
    rawRow.getColumnVal(1),
    rawRow.getColumnVal(2),
    Integer.valueOf(rawRow.getColumnVal(3))
  )
}
val customerRows = customerRawRows.map(customerConversionFunc)
val customerCols = Array(
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("name", DataTypes.StringType, false, Metadata.empty),
  new StructField("address", DataTypes.StringType, false, Metadata.empty),
  new StructField("age", DataTypes.IntegerType, false, Metadata.empty)
)
val customerSchema = new StructType(customerCols)


val transactionRawRows = SegmentLoader.getRawRows(sc,"Transactions",clientProps,csvColumnifier);
val transactionConversionFunc = new org.apache.spark.api.java.function.Function[RawRow, Row] {
  final val serialVersionUID = -812004521983071103L
  override def call(rawRow: RawRow) : Row = RowFactory.create(
    Integer.valueOf(rawRow.getColumnVal(0)),
    Integer.valueOf(rawRow.getColumnVal(1))
  )
}
val transactionRows = transactionRawRows.map(transactionConversionFunc)
val transactionCols = Array(
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty)
)
val transactionSchema = new StructType(transactionCols)


val itemRawRows = SegmentLoader.getRawRows(sc,"Items",clientProps,csvColumnifier);
val itemConversionFunc = new org.apache.spark.api.java.function.Function[RawRow, Row] {
  final val serialVersionUID = -812004521983071103L
  override def call(rawRow: RawRow) : Row = RowFactory.create(
    Integer.valueOf(rawRow.getColumnVal(0)),
    rawRow.getColumnVal(1),
    java.lang.Double.valueOf(rawRow.getColumnVal(2))
  )
}
val itemRows = itemRawRows.map(itemConversionFunc)
val itemCols = Array(
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("description", DataTypes.StringType, false, Metadata.empty),
  new StructField("price", DataTypes.DoubleType, false, Metadata.empty)
)
val itemSchema = new StructType(itemCols)


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