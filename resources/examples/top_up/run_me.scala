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

val initialThresholds = Map(
  0 -> 0L,
  1 -> 0L,
  2 -> 0L,
  3 -> 0L,
  4 -> 0L,
  5 -> 0L
)

// load 2nd offset+ from each partition
val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  rowPredicates = Array(OffsetPredicate.buildGreaterThanRowPredicate(initialThresholds)))

// confirm 6 rows read (there are 10 customers and 6 partitions so this should read the last 4)
// all offsets should be above 0
customerRawRows.collect.map(i => i.getLongVal(0))

// lets get the latest offset we have read for each partition in a map (this will be used as input to the next predicate)
val latestThresholds = (customerRawRows.collect.map(i=>(i.getIntVal(1).asInstanceOf[Int],i.getLongVal(0).asInstanceOf[Long])).toSet ++ initialThresholds.toSet).groupBy(_._1).mapValues(i => i.map(_._2).max).map(identity)

// let's add 2 more customers (run this in the master container):
// echo 'batch.size=10' > producer.properties
// echo 'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner' >> producer.properties
// kafka-console-producer --broker-list worker1:9091 --topic Customers_json --producer.config producer.properties < /home/ubuntu/resources/Customers_json2.txt

// now lets top up
val topUpRowRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  rowPredicates = Array(OffsetPredicate.buildGreaterThanRowPredicate(latestThresholds)))

// this should fetch only 2 rows each with offsets higher than those in the latestThresholds map
topUpRowRawRows.collect.map(i => (i.getIntVal(1),i.getLongVal(0)))

// as the new message were written to a new segment (we have a very low segment.ms on this topic). We should be able to achieve the same result with a segment predicate
val topUpSegmentRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  segmentPredicates = Array(OffsetPredicate.buildGreaterThanSegmentPredicate(latestThresholds)))

// this should fetch the same 2 rows:
topUpSegmentRawRows.collect.map(i => (i.getIntVal(1),i.getLongVal(0)))

// row and segment predicates can (and should) be used in conjunction with eachother
val topUpRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  rowPredicates = Array(OffsetPredicate.buildGreaterThanRowPredicate(latestThresholds)),
  segmentPredicates = Array(OffsetPredicate.buildGreaterThanSegmentPredicate(latestThresholds)))

// this should fetch only 2 rows each with offsets higher than those in the latestThresholds map
topUpRawRows.collect.map(i => (i.getIntVal(1),i.getLongVal(0)))

// now we union the original and top up rdds
val allRows = sc.union(customerRawRows,topUpRawRows)

customerRawRows.collect.size
topUpRawRows.collect.size

// allRows size is the sum of customerRawRows and topUpRawRows
allRows.collect.size
