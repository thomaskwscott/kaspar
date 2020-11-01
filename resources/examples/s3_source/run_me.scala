import org.apache.spark.sql.SQLContext

import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.JsonColumnifier
import kaspar.dataload.structure.RawRow
import kaspar.dataload.predicate.MinMaxPredicate
import kaspar.dataload.predicate.OffsetPredicate
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType

val clientProps = new java.util.Properties
clientProps.setProperty("bootstrap.servers","worker1:9091")

val customersColumnMappings =   Array(
  ("id",ColumnType.INTEGER),
  ("name",ColumnType.STRING),
  ("address",ColumnType.STRING),
  ("age",ColumnType.INTEGER)
)
val customersColumnifier = new JsonColumnifier(
  customersColumnMappings
)

val itemsColumnMappings = Array(
  ("id",ColumnType.INTEGER),
  ("name",ColumnType.STRING),
  ("price",ColumnType.DOUBLE)
)
val itemsColumnifier = new JsonColumnifier(
  itemsColumnMappings
)

val transactionsColumnMappings = Array(
  ("customer_id",ColumnType.INTEGER),
  ("item_id",ColumnType.INTEGER),
)
val transactionsColumnifier = new JsonColumnifier(
  transactionsColumnMappings
)

val accessKey = sys.env("AWS_ACCESS_KEY_ID")
val secret = sys.env("AWS_SECRET_ACCESS_KEY")
val region = "eu-west-1"
val bucketName = "kaspar"
val s3objects = Array("00000000000000000000.log")

val customerRawRows = TopicLoader.getRawRowsFromS3(sc,customersColumnifier,accessKey, secret, region,
  bucketName, s3objects)

val transactionRawRows = TopicLoader.getRawRowsFromKafka(sc,"Transactions_json",clientProps,transactionsColumnifier)

val itemRawRows = TopicLoader.getRawRowsFromKafka(sc,"Items_json",clientProps,itemsColumnifier)

val sqlContext = new SQLContext(sc)

TopicLoader.registerTableFromRdd(sqlContext,customerRawRows,"Customers",customersColumnMappings)
TopicLoader.registerTableFromRdd(sqlContext,transactionRawRows,"Transactions",transactionsColumnMappings)
TopicLoader.registerTableFromRdd(sqlContext,itemRawRows,"Items",itemsColumnMappings)

val sql =
  """
    | select Customers.name,Items.name,price
    | from Customers
    | join Transactions
    | on Customers.id = Transactions.customer_id
    | join Items
    | on Items.id = Transactions.item_id
    |""".stripMargin

sqlContext.sql(sql).show(100)