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

// create an index for age column on the segment that contains these rows
// rather annoyingly we must add 3 to the index of the column in the column mappings
val indexReport = TopicLoader.createIndex(sc,"Customers_json",clientProps,customersColumnifier,
  MinMaxPredicate.getIndexName(),MinMaxPredicate.getIndexFunction(Seq((6,ColumnType.INTEGER))))

// should be 12
indexReport.collect.size

// segment predicate example, this uses the previously created index files to read only segments that contain customers
// aged above 80
val customerSegmentPredicateRawRowsHigh = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanSegmentPredicate(80,6)))

// this should show 0 rows as the segments contain no rows with that value
customerSegmentPredicateRawRowsHigh.collect.size

// this example is much less deterministic. We know that we have one row with an age higher than 45, the segment
// predicate will read all segments that contain this rows (and all rows in the segment). Depending on the way the
// messages are partitions in kafka this could return 1 or more rows.
// aged above 45
val customerSegmentPredicateRawRowsLow = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanSegmentPredicate(45,6)))

// this should show >0 rows as there is at least 1 segment containing a row with age >45
customerSegmentPredicateRawRowsLow.collect.size


