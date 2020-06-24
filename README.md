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
 2. Spark schedules each task to a worker colocated with a broker. The first thing this task does is check the broker id
  from the Kafka broker's configuration against the the broker id associated with the task. If these do not match the 
  task is failed and will be rescheduled by Spark
 3. If the broker ids match then the task will read the data directly from disk (using a method similar to Kafka's 
 dumpLogSegments command) and return a RDD containing the message values.
 4. From here on in Spark can do the rest, possibly  adding a schema and running SparkSQL?   

### Creating rows from messages:

Messages are converted to RawRow (kaspar.dataload.structure.RawRow) objects which wrap an array of strings (1 
string per column) representing a row. This conversion happens during the read from disk process. This is done using 
a Columnifier (kaspar.dataload.structure.Columnifier). The only implementation of this available right now is the 
CSVColumnifier which splits the messages on a delimiter. The first 2 columns (rawRow.getColumnVal(0) and 
rawRow.getColumnVal(1)) are currently reserved for offset and timestamps of the message. 

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

### Pushing Predicates down

One of the advantages of this approach is that we can remove messages that are not required for the query at the read 
stage so that they never go across the network or through a broker request pipeline. Kaspar allows you to define 
Predicates that are pushed down and apply to the RawRow object values read from disk. These are standard Scala 
functions e.g.:

```
val customerRawRows = TopicLoader.getRawRows(sc,"Customers",clientProps,csvColumnifier,
  (rawRow: RawRow) => rawRow.getColumnVal(3).startsWith("B"))
```

Any number of these can be applied as varargs to TopicLoader.getRawRows(). An example of this can be seen on line 21 
of spark-shell_example.scala where a predicate is applied to reduce the Customers dataframe to only customers whose 
names start with the letter "B" 

## Installing
 
Build images with spark worker/confluent:

```
cd docker
./make-images.sh
```

## Running with Spark Shell

Kaspar can be used with Spark Shell:

```
docker-compose exec master bash
cd /home/ubuntu/bin
./setup.sh
./launchShell.sh
```

There is a far more interesting demo available in resources/spark-shell_example.scala. To run just copy the entirety of 
this file and paste it into your Spark shell session

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
