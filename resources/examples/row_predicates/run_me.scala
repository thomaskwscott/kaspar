import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.SimpleJsonValueColumnifier
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

// no predicates
val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier)

// 10 rows
customerRawRows.collect.size

// row predicate example, this fetches all customers with a name that start with 'B'
val customerRowPredicateRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  rowPredicates = Array((rawRow: RawRow) => rawRow.getColumnVal(4).toString().startsWith("B")))

// this should show only 2 rows
customerRowPredicateRawRows.collect.size

