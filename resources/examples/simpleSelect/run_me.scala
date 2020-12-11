import org.apache.spark.sql.SQLContext

import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.JsonColumnifier
import kaspar.dataload.structure.RawRow
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

val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier)

val sqlContext = new SQLContext(sc)

TopicLoader.registerTableFromRdd(sqlContext,customerRawRows,"Customers",customersColumnMappings)

val customersSql =
  """
    | select *
    | from Customers
    |""".stripMargin

sqlContext.sql(customersSql).show(100)

val itemsColumnMappings = Array(
  ("id",ColumnType.INTEGER),
  ("name",ColumnType.STRING),
  ("price",ColumnType.DOUBLE)
)
val itemsColumnifier = new JsonColumnifier(
  itemsColumnMappings
)

val itemRawRows = TopicLoader.getRawRowsFromKafka(sc,"Items_json",clientProps,itemsColumnifier)

TopicLoader.registerTableFromRdd(sqlContext,itemRawRows,"Items",itemsColumnMappings)

val itemsSql =
  """
    | select *
    | from Items
    |""".stripMargin

sqlContext.sql(itemsSql).show(100)