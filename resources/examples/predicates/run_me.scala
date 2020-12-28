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

// segment predicate example, this uses the previously created index files to read only segments that contain customers
// aged above 30
val customerSegmentPredicateRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanSegmentPredicate(30,6)))

// this should show 10 rows
customerSegmentPredicateRawRows.collect.size

// let's print the names and ages of all those 10 retrieved customers, we see that there are some customers still in
// the list that are below the predicate age. This is because segment predicates will read the entire segment is any
// row inside the segment matches. Further filtering must be done with row predicates.
customerSegmentPredicateRawRows.collect.map(i=>(i.getStringVal(4),i.getIntVal(6)))

// for completeness let's rerun the fetch with a predicate that matches no records
val customerSegmentPredicateUnMatchedRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanSegmentPredicate(112,6)))

// this should show 0 rows
customerSegmentPredicateUnMatchedRawRows.collect.size

