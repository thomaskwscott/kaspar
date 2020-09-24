# Kaspar

Kafka/Spark integration

## What is Kaspar

Kaspar is a way to bring Spark's computational capabilities closer to Kafka data than the traditional consume pattern. 
Spark resources and Kafka resources are co located to provide fast access to the underlying Kafka data without going 
over the network.

## How it works

### Assembling the data

Nodes with Spark Workers and Kafka brokers should be deployed with a separate Spark Master:

```
+--------+  +--------+  +--------+  +--------+
| Kafka  |  | Kafka  |  | Kafka  |  | Spark  |
|        |  |        |  |        |  | Master |
| Spark  |  | Spark  |  | Spark  |  |        |
| Worker |  | Worker |  | Worker |  |        |
+--------+  +--------+  +--------+  +--------+
```

Kaspar then loads RDDs from the Kafka data directory colocated with the worker to which the Spark tasks are assigned. If
 the task is scheduled on a node that does not have the required data then the task is failed and Spark will reschedule 
it on another node until the correct node is found. Kaspar takes advantage of Spark's blacklisting features to ensure 
that the task is not rescheduled to the same node twice.

The data load process:

 1. Use a Kafka AdminClient to determine which brokers have the data we are looking for. An RDD is created that contains
 entries for each broker and the partitions to be fetched from that broker. This RDD is partitioned so that each entry 
 will run as it's own task.
 2. Spark schedules each task to a worker colocated with a broker. Spark's preferredLocation features are used to guide 
 this scheduling however this is not garunteed. For this reason, the first thing this task does is check executor's host 
 name against the kafka partition's ISR host list (This does not have to be leader as Kaspar can satisfy it's read 
 requirements from replicas. If the executor host is not in the ISR list then the task is failed and will be rescheduled by 
 Spark until it finds an appropriate host to execute on.
 3. Once the above is satisfied, the task will read the data directly from disk (using a method similar to Kafka's 
 dumpLogSegments command) and return a RDD containing the message values.
 4. From here on in Spark can do the rest, possibly  adding a schema and running SparkSQL?   

### Creating rows from messages(csv):

Messages are converted to RawRow (kaspar.dataload.structure.RawRow) objects which wrap an array of strings (1 
string per column) representing a row. This conversion happens during the read from disk process. This is done using 
a Columnifier (kaspar.dataload.structure.Columnifier). This section describes using the CSVColumnifier which splits 
the messages into Strings based on a delimiter. The first 2 columns (rawRow.getColumnVal(0) and 
rawRow.getColumnVal(1)) are currently reserved for offset and timestamps of the message. 

RawRows are fetched using the `TopicLoader.getRawRows()` method. This takes a number of parameters relevant to the 
underlying kafka topic, any predicates to be applied at read time and the Columnifier used to convert messages to 
RawRow objects.

```
val transactionRawRows = TopicLoader.getRawRows(sc,"Transactions",clientProps,csvColumnifier)
```

The RawRows must then be converted into SparkSQL Row format using a conversion function such as the one below:

```
val transactionRows = transactionRawRows.map(rawRow => RowFactory.create(
  rawRow.getColumnVal(0),
  rawRow.getColumnVal(1),
  Integer.valueOf(rawRow.getColumnVal(2)),
  Integer.valueOf(rawRow.getColumnVal(3))
))
```

It is here that column types should be applied. 

Next we define a schema to apply to to our set of rows:

```
val transactionCols = Array(
  new StructField("offset", DataTypes.StringType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.StringType, false, Metadata.empty),
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("itemId", DataTypes.IntegerType, false, Metadata.empty)
)
val transactionSchema = new StructType(transactionCols)
```

Note the types defined in the schema must match the underlying data.

Finally we define a dataframe with table data and schema for use in sql queries:

```
val transactionDf = sqlContext.createDataFrame(transactionRows,transactionSchema)
transactionDf.createOrReplaceTempView("Transactions")
```

### Creating rows from messages(json):

An alternative Columnifier for working with JSON messages is provided in the JsonColumnifier. This maps 
fields in json messages to columns (and their appropriate types) The first 2 columns (rawRow.getLongVal(0) and 
rawRow.getLongVal(1)) are currently reserved for offset and timestamps of the message in Long format when using 
JSON messages. 

A JsonColumnifier must be passed a mapping of strings and column types as below:

```
val customersColumnifier = new JsonColumnifier(
  Array(
    ("id",ColumnType.INTEGER),
    ("name",ColumnType.STRING),
    ("address",ColumnType.STRING),
    ("age",ColumnType.INTEGER)
  )
)
```

The above example will extract the field "id" from the input Json and map it to a column of type INTEGER.

The ordering of these fields is important as it represents the results column indexes that can be used to retrieve the 
parsed data. For example the "name" field maps to column index 3 (0 and 1 are reserved for offset and timestamp).  
 
The RawRows must then be converted into SparkSQL Row format using a conversion function such as the one below:

```
val customerRows = customerRawRows.map(rawRow => RowFactory.create(
  rawRow.getLongVal(0),
  rawRow.getLongVal(1),
  rawRow.getIntVal(2),
  rawRow.getStringVal(3),
  rawRow.getStringVal(4),
  rawRow.getIntVal(5)
))
```

Note that the right accesor should be used to match the typ of the data. To quote our previous example, "name" is a 
String type at index 3 and so the correct accessor is: `rawRow.getStringVal(3)`

Next we define a schema to apply to to our set of rows just as we would with CSV data:

```
val customerCols = Array(
  new StructField("offset", DataTypes.LongType, false, Metadata.empty),
  new StructField("timestamp", DataTypes.LongType, false, Metadata.empty),
  new StructField("customerId", DataTypes.IntegerType, false, Metadata.empty),
  new StructField("name", DataTypes.StringType, false, Metadata.empty),
  new StructField("address", DataTypes.StringType, false, Metadata.empty),
  new StructField("age", DataTypes.IntegerType, false, Metadata.empty)
)
val customerSchema = new StructType(customerCols)
```

Note the types defined in the schema must match the underlying data.

Finally we define a dataframe with table data and schema for use in sql queries:

```
val customerDf = sqlContext.createDataFrame(customerRows,customerSchema)
customerDf.createOrReplaceTempView("Customers")
```

### Pushing Predicates down

One of the advantages of this approach is that we can remove messages that are not required for the query at the read 
stage so that they never go across the network or through a broker request pipeline. Kaspar allows you to define 
Row Predicates that are pushed down and apply to the RawRow object values read from disk. These are standard Scala 
functions e.g.:

```
val customerRawRows = TopicLoader.getRawRows(sc,dataDir,serverProperties,"Customers",clientProps,csvColumnifier,
  rowPredicates = Array((rawRow: RawRow) => rawRow.getColumnVal(3).startsWith("B")))
```

Any number of these can be applied in the rowPredicates arg to TopicLoader.getRawRows(). An example of this can be seen  
in spark-shell_example.scala where a predicate is applied to reduce the Customers dataframe to only customers whose 
names start with the letter "B"

Kaspar also supports segment predicates. These are designed to use indexes created for the immutable segment files read 
by Kaspar. These predicates are used to determine whether a segment will be read or ignored during query execution. An 
example can be seen here:

```
val customerRawRows = TopicLoader.getRawRows(sc,dataDir,serverProperties,"Customers",clientProps,csvColumnifier,
  segmentPredicates = Array(MinMaxPredicate.buildGreaterThanEqualSegmentPredicate(30,5)))
```  

Note: A segment predicate only determines whether or not a segment file should be read and does not filter the rows 
inside the segment. For instance, the above reads an index that contains the max and min values for customer age in 
the segments, if the segment contains at least 1 row with and age higher than 30 then the segment will be read. However
if no row predicate is supplied this read will also include rows in the segment with age <= 30.

## Installing
 
Build images with spark worker/confluent:

```
cd docker
./make-images.sh
```

## Running with Spark Shell

Kaspar can be used with Spark Shell:

```
docker-compose up -d
docker-compose exec master bash
cd /home/ubuntu/bin
./setup.sh
./launchShell.sh
```

There is a far more interesting demo available in resources/spark-shell_example.scala. To run just copy the entirety of 
this file and paste it into your Spark shell session

Note, if you wish to use segmentPredicates then create some simple indexes by running:

```
./make_index.sh
```

This demo uses 3 underlying topics, Items, Customers and Transactions. Each of these topics contain row data e.g.
 
```
0,Bruce Wayne,Wayne Manor,36
```

You can see this raw data in the resources folder.

The demo reads and established these 3 topics as Spark Dataframes before joining them using Spark SQL with the following query:

```
 select name,price
 from Customers
 join Transactions
 on Customers.customerId = Transactions.customerId
 join Items
 on Items.itemId = Transactions.itemId
```
