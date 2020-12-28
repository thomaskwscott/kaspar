import org.apache.spark.sql.SQLContext

import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.SimpleJsonValueColumnifier
import kaspar.dataload.structure.PathJsonValueColumnifier
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
val customersColumnifier = new SimpleJsonValueColumnifier(
  customersColumnMappings
)

val itemsColumnMappings = Array(
  ("id",ColumnType.INTEGER,"$.id"),
  ("name",ColumnType.STRING,"$.name"),
  ("price",ColumnType.DOUBLE,"$.price"),
  ("margin",ColumnType.DOUBLE,"$.internal.margin")
)
val itemsColumnifier = new PathJsonValueColumnifier(
  itemsColumnMappings
)

val transactionsColumnMappings = Array(
  ("customer_id",ColumnType.INTEGER),
  ("item_id",ColumnType.INTEGER),
)
val transactionsColumnifier = new SimpleJsonValueColumnifier(
  transactionsColumnMappings
)

val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier)

val transactionRawRows = TopicLoader.getRawRowsFromKafka(sc,"Transactions_json",clientProps,transactionsColumnifier)

val itemRawRows = TopicLoader.getRawRowsFromKafka(sc,"Items_json",clientProps,itemsColumnifier)

val sqlContext = new SQLContext(sc)

TopicLoader.registerTableFromRdd(sqlContext,customerRawRows,"Customers",customersColumnMappings)
TopicLoader.registerTableFromRdd(sqlContext,transactionRawRows,"Transactions",transactionsColumnMappings)
TopicLoader.registerTableFromRdd(sqlContext,itemRawRows,"Items",itemsColumnMappings.map(i => (i._1,i._2)))

val sql =
  """
    | select Customers.name,Items.name,price,margin
    | from Customers
    | join Transactions
    | on Customers.id = Transactions.customer_id
    | join Items
    | on Items.id = Transactions.item_id
    |""".stripMargin

sqlContext.sql(sql).show(100)