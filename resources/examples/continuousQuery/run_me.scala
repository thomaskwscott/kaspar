import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.SimpleJsonValueColumnifier
import kaspar.dataload.structure.RawRow
import kaspar.dataload.predicate.MinMaxPredicate
import kaspar.dataload.predicate.OffsetPredicate
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType

import org.apache.spark.sql.SQLContext

import java.util.Calendar

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

val sqlContext = new SQLContext(sc)

// these tables are static for this demo
val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier)
val itemRawRows = TopicLoader.getRawRowsFromKafka(sc,"Items_json",clientProps,itemsColumnifier)

TopicLoader.registerTableFromRdd(sqlContext,customerRawRows,"Customers",customersColumnMappings)
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


// set the starting point
var transactionsThresholds = Map(
  0 -> -1L,
  1 -> -1L,
  2 -> -1L,
  3 -> -1L,
  4 -> -1L,
  5 -> -1L
)

while(true) {
  val transactionRawRows = TopicLoader.getRawRowsFromKafka(sc,"Transactions_json",clientProps,transactionsColumnifier,
    rowPredicates = Array(OffsetPredicate.buildGreaterThanRowPredicate(transactionsThresholds)),
    segmentPredicates = Array(OffsetPredicate.buildGreaterThanSegmentPredicate(transactionsThresholds)))
  TopicLoader.registerTableFromRdd(sqlContext, transactionRawRows, "Transactions", transactionsColumnMappings)

  // display new rows
  if(!transactionRawRows.isEmpty) {
    println(Calendar.getInstance().getTime())
    sqlContext.sql(sql).show(100)
    transactionsThresholds = (transactionRawRows.collect.map(i => (i.getIntVal(1).asInstanceOf[Int], i.getLongVal(0).asInstanceOf[Long])).toSet ++ transactionsThresholds.toSet).groupBy(_._1).mapValues(i => i.map(_._2).max).map(identity)
  }

  Thread.sleep(1000)
}
