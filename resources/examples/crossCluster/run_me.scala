import org.apache.spark.sql.SQLContext

import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.SimpleJsonValueColumnifier
import kaspar.dataload.structure.RawRow
import kaspar.dataload.predicate.MinMaxPredicate
import kaspar.dataload.predicate.OffsetPredicate
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType

val clientProps = new java.util.Properties
clientProps.setProperty("bootstrap.servers","worker1:9091")

val altClientProps = new java.util.Properties
altClientProps.setProperty("bootstrap.servers","alt-worker1:10091")

val customersColumnMappings =   Array(
  ("id",ColumnType.INTEGER),
  ("name",ColumnType.STRING),
  ("address",ColumnType.STRING),
  ("age",ColumnType.INTEGER)
)
val customersColumnifier = new SimpleJsonValueColumnifier(
  customersColumnMappings
)

val itemsColumnMappings = Array(
  ("id",ColumnType.INTEGER),
  ("name",ColumnType.STRING),
  ("price",ColumnType.DOUBLE)
)
val itemsColumnifier = new SimpleJsonValueColumnifier(
  itemsColumnMappings
)

val transactionsColumnMappings = Array(
  ("customer_id",ColumnType.INTEGER),
  ("item_id",ColumnType.INTEGER),
)
val transactionsColumnifier = new SimpleJsonValueColumnifier(
  transactionsColumnMappings
)

val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Alt_customers_json",altClientProps,customersColumnifier)

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