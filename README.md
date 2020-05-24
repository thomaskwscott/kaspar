# Kaspar

Kafka/Spark integration

## What is Kaspar

Kaspar is a way to bring Spark's computational capabilities closer to Kafka data than the traditional consume pattern. 
Spark resources and Kafka resources are co located to provide fast access to the underlying Kafka data without going 
over the network.

## How it works

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

## Installing
 
Build images with spark worker/confluent:

```
cd docker
./make-images.sh
```

## Running the demo

Demo source code can be found in the demo directory.

The demo creates 2 kafka topics:

* testTopic - contains integers 1 to 100
* testTopic2 - contains integers 1 to 25

Kaspar is then used to read the values from both of these topics and join them using SparkSQL. The output should be 2 
columns both containing the numbers 1 to 25

```
docker-compose exec master bash
cd /home/ubuntu/bin
./setup.sh
./launchDemo.sh
```

## Running Spark Shell

Kaspar can also be used with Spark Shell:

```
docker-compose exec master bash
cd /home/ubuntu/bin
./launchShell.sh
```

Some example code can be seen here:

```
    import io.confluent.dataload.SegmentLoader
    val clientProps = new java.util.Properties
    clientProps.setProperty("bootstrap.servers","worker1:9091")
    val testRows = SegmentLoader.getRawRows(sc,"testTopic2",clientProps)
    val collectedRows = testRows.collect()
```