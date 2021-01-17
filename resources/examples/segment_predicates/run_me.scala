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

// this shows 0 why?
indexReport.collect.size

// we don't index the active segment and all segments currently associated with Customers_json are active.
// We need to add some more dummy records to force ta segment roll
// from the master container (docker-compose exec master bash):
// cd /home/ubuntu/bin/
// ./add_dummy_customers.sh
// this will add 6 dummy customers (1 per partition) to force a sgement roll in each Customers_json partition
// because these new dummy records will be in the new active segment they will always be returned in our
// queries and we must filter them out to show the segment predicates

// let's create the index again:
val newIndexReport = TopicLoader.createIndex(sc,"Customers_json",clientProps,customersColumnifier,
  MinMaxPredicate.getIndexName(),MinMaxPredicate.getIndexFunction(Seq((6,ColumnType.INTEGER))))

// this shows >0
newIndexReport.collect.size

// segment predicate example, this uses the previously created index files to read only segments that contain customers
// aged above 80
val customerSegmentPredicateRawRowsHigh = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanSegmentPredicate(80,6)))

// this should show 0 rows as the segments contain no rows with that value (filtering out dummy records)
customerSegmentPredicateRawRowsHigh.collect.filter(_.getStringVal(4) != "Dummy").size

// this example is much less deterministic. We know that we have one row with an age higher than 45, the segment
// predicate will read all segments that contain this rows (and all rows in the segment). Depending on the way the
// messages are partitions in kafka this could return 1 or more rows.
// aged above 45
val customerSegmentPredicateRawRowsLow = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanSegmentPredicate(45,6)))

// this should show >0 rows as there is at least 1 segment containing a row with age >45 (filtering out dummy records)
customerSegmentPredicateRawRowsLow.collect.filter(_.getStringVal(4) != "Dummy").size


