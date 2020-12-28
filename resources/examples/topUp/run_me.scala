import kaspar.dataload.TopicLoader
import kaspar.dataload.structure.SimpleJsonValueColumnifier
import kaspar.dataload.structure.RawRow
import kaspar.dataload.predicate.MinMaxPredicate
import kaspar.dataload.predicate.OffsetPredicate
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType

import org.apache.spark.sql.SQLContext

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

val initialThresholds = Map(
  0 -> -1L,
  1 -> -1L,
  2 -> -1L,
  3 -> -1L,
  4 -> -1L,
  5 -> -1L
)

val customerRawRows = TopicLoader.getRawRowsFromKafka(sc,"Customers_json",clientProps,customersColumnifier,
  rowPredicates = Array(OffsetPredicate.buildGreaterThanRowPredicate(initialThresholds)))

val sqlContext = new SQLContext(sc)

TopicLoader.registerTableFromRdd(sqlContext,customerRawRows,"Customers",customersColumnMappings)

val customersSql =
  """
    | select *
    | from Customers
    |""".stripMargin

// 10 rows
sqlContext.sql(customersSql).show(100)

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

TopicLoader.registerTableFromRdd(sqlContext,allRows,"Customers",customersColumnMappings)

// 12 rows now
sqlContext.sql(customersSql).show(100)