package kaspar.dataload

import kaspar.dataload.metadata.{ColumnType, Location, TaskAssignment}
import kaspar.dataload.structure.{CsvRowDeserializer, RawRow}
import org.apache.kafka.clients.admin.{AdminClient, DescribeClusterResult, DescribeLogDirsResult, DescribeTopicsResult, TopicDescription}
import org.apache.kafka.common.{KafkaFuture, Node, TopicPartitionInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.{SparkConf, SparkContext, SparkException, sql}
import org.easymock.Capture
import org.easymock.EasyMock.{anyObject, capture, expect, expectLastCall, replay}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.easymock.EasyMockSugar.mock

import java.io.File
import java.util.{Collections, Properties}
import scala.collection.mutable.ArrayBuffer

class KasparDriverTest extends AnyFlatSpec {

  private val dataDir = "src/test/resources/fakeBroker"

  "A KasparDriver" should "throw RuntimeException if getRows is executed on the wrong node" in {
    val kasparDriver = new KasparDriver(new Properties())

    val topic = "someTopic"
    val rowDeserializer = new CsvRowDeserializer
    val mockSparkContext = mock[SparkContext]
    val mockAdminClient = mock[AdminClient]
    val mockDescribeClusterResult = mock[DescribeClusterResult]
    val mockDescribeLogDirsResult = mock[DescribeLogDirsResult]
    val mockDescribeTopicsResult = mock[DescribeTopicsResult]
    val mockTopicDescription = mock[TopicDescription]

    val brokerNode = new Node(
      0,
      "someNode",
      9092
    )

    val taskAssignment = TaskAssignment(
      0,
      Seq(Location("someNode", 0, ArrayBuffer(dataDir)))
    )

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("someApp")
    val realSpark = SparkContext.getOrCreate(sparkConf)

    val realRDD = realSpark.parallelize(Seq(taskAssignment))

    expect(mockAdminClient.describeCluster()).andReturn(mockDescribeClusterResult)
    expect(mockDescribeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonList(brokerNode)))
    expect(mockAdminClient.describeLogDirs(anyObject())).andReturn(mockDescribeLogDirsResult)
    expect(mockDescribeLogDirsResult.all()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonMap(0,Collections.singletonMap(dataDir, null))
    ))
    expect(mockAdminClient.describeTopics(Collections.singletonList(topic))).andReturn(mockDescribeTopicsResult)
    expect(mockDescribeTopicsResult.all()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonMap(topic, mockTopicDescription)
    ))
    expect(mockTopicDescription.partitions()).andReturn(Collections.singletonList(new TopicPartitionInfo(
      0,
      brokerNode,
      Collections.singletonList(brokerNode),
      Collections.singletonList(brokerNode)
    )))
    expect(mockSparkContext.makeRDD(
      Seq(
        (
          taskAssignment,
          Seq("someNode")
        )
      )
     )).andReturn(realRDD)

    replay(
      mockSparkContext,
      mockAdminClient,
      mockDescribeClusterResult,
      mockDescribeLogDirsResult,
      mockDescribeTopicsResult,
      mockTopicDescription
    )

    kasparDriver.adminClient = mockAdminClient
    val rowsRdd = kasparDriver
      .getRows(mockSparkContext, topic, rowDeserializer, Array.empty, Array.empty)

    assertThrows[RuntimeException] {
      try {
        rowsRdd.collect()
      } catch {
        case e: SparkException => {
          val cause = e.getCause
          assert(cause.getMessage.startsWith("Ignore this"))
          throw cause
        }
      }

    }

  }

  "A KasparDriver" should "retrieve rows from segment files" in {
    val kasparDriver = new KasparDriver(new Properties())

    val topic = "someTopic"
    val rowDeserializer = new CsvRowDeserializer
    val mockSparkContext = mock[SparkContext]
    val mockAdminClient = mock[AdminClient]
    val mockDescribeClusterResult = mock[DescribeClusterResult]
    val mockDescribeLogDirsResult = mock[DescribeLogDirsResult]
    val mockDescribeTopicsResult = mock[DescribeTopicsResult]
    val mockTopicDescription = mock[TopicDescription]

    val host = java.net.InetAddress.getLocalHost().getHostName();

    val brokerNode = new Node(
      0,
      host,
      9092
    )

    val taskAssignment = TaskAssignment(
      0,
      Seq(Location(host, 0, ArrayBuffer(dataDir)))
    )

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("someApp")
    val realSpark = SparkContext.getOrCreate(sparkConf)

    val realRDD = realSpark.parallelize(Seq(taskAssignment))

    expect(mockAdminClient.describeCluster()).andReturn(mockDescribeClusterResult)
    expect(mockDescribeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonList(brokerNode)))
    expect(mockAdminClient.describeLogDirs(anyObject())).andReturn(mockDescribeLogDirsResult)
    expect(mockDescribeLogDirsResult.all()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonMap(0,Collections.singletonMap(dataDir, null))
    ))
    expect(mockAdminClient.describeTopics(Collections.singletonList(topic))).andReturn(mockDescribeTopicsResult)
    expect(mockDescribeTopicsResult.all()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonMap(topic, mockTopicDescription)
    ))
    expect(mockTopicDescription.partitions()).andReturn(Collections.singletonList(new TopicPartitionInfo(
      0,
      brokerNode,
      Collections.singletonList(brokerNode),
      Collections.singletonList(brokerNode)
    )))
    expect(mockSparkContext.makeRDD(
      Seq(
        (
          taskAssignment,
          Seq(host)
        )
      )
    )).andReturn(realRDD)

    replay(
      mockSparkContext,
      mockAdminClient,
      mockDescribeClusterResult,
      mockDescribeLogDirsResult,
      mockDescribeTopicsResult,
      mockTopicDescription
    )

    kasparDriver.adminClient = mockAdminClient
    val rowsRdd = kasparDriver
      .getRows(mockSparkContext, topic, rowDeserializer, Array.empty, Array.empty)

    val rows = rowsRdd.collect()

    assert(rows.length == 2)
    assert(rows(0).getStringVal(0) == "0")
    assert(rows(0).getStringVal(1) == "0")
    // skip timestamp verification
    assert(rows(0).getStringVal(3) == "key")
    assert(rows(0).getStringVal(4) == "val")


  }

  "A KasparDriver" should "register a table with internal columns" in {
    val kasparDriver = new KasparDriver(new Properties())

    val tableName = "someTable"
    val mockSqlContext = mock[SQLContext]
    val mockDataFrame = mock[sql.DataFrame]

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("someApp")
    val realSpark = SparkContext.getOrCreate(sparkConf)

    val rawRow = new RawRow;
    val realRDD = realSpark.parallelize(Seq(rawRow))

    val structTypeCapture = new Capture[StructType]()

    expect(mockSqlContext.createDataFrame(anyObject[RDD[Row]],capture[StructType](structTypeCapture))).andReturn(mockDataFrame)
    mockDataFrame.createOrReplaceTempView(tableName)
    expectLastCall()

    replay(
      mockSqlContext
    )

    val columnMappings =   Array(
      ("someCol",ColumnType.INTEGER)
    )

    kasparDriver.registerTable(
      mockSqlContext,
      realRDD,
      tableName,
      columnMappings
    )

    val captured = structTypeCapture.getValue
    assert(captured.fields.length == 4)
    assert(captured.fields(0).name == "_offset")
    assert(captured.fields(0).dataType == DataTypes.LongType)
    assert(captured.fields(1).name == "_partition")
    assert(captured.fields(1).dataType == DataTypes.IntegerType)
    assert(captured.fields(2).name == "_timestamp")
    assert(captured.fields(2).dataType == DataTypes.LongType)
  }

  "A KasparDriver" should "create a simple index file" in {
    val kasparDriver = new KasparDriver(new Properties())

    val topic = "someTopic"
    val indexName = "someIndexName"
    val indexFunction = {records: Seq[RawRow] => "someIndexVal"}
    val rowDeserializer = new CsvRowDeserializer
    val mockSparkContext = mock[SparkContext]
    val mockAdminClient = mock[AdminClient]
    val mockDescribeClusterResult = mock[DescribeClusterResult]
    val mockDescribeLogDirsResult = mock[DescribeLogDirsResult]
    val mockDescribeTopicsResult = mock[DescribeTopicsResult]
    val mockTopicDescription = mock[TopicDescription]

    // clean any existing index file
    val indexFileName = dataDir + "/" + topic + "-0/00000000000000000000." + indexName
    val segmentFileName = dataDir + "/" + topic + "-0/00000000000000000000.log"
    new File(indexFileName).delete()

    val host = java.net.InetAddress.getLocalHost().getHostName();

    val brokerNode = new Node(
      0,
      host,
      9092
    )

    val taskAssignment = TaskAssignment(
      0,
      Seq(Location(host, 0, ArrayBuffer(dataDir)))
    )

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("someApp")
    val realSpark = SparkContext.getOrCreate(sparkConf)

    val realRDD = realSpark.parallelize(Seq(taskAssignment))

    expect(mockAdminClient.describeCluster()).andReturn(mockDescribeClusterResult)
    expect(mockDescribeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonList(brokerNode)))
    expect(mockAdminClient.describeLogDirs(anyObject())).andReturn(mockDescribeLogDirsResult)
    expect(mockDescribeLogDirsResult.all()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonMap(0,Collections.singletonMap(dataDir, null))
    ))
    expect(mockAdminClient.describeTopics(Collections.singletonList(topic))).andReturn(mockDescribeTopicsResult)
    expect(mockDescribeTopicsResult.all()).andReturn(KafkaFuture.completedFuture(
      Collections.singletonMap(topic, mockTopicDescription)
    ))
    expect(mockTopicDescription.partitions()).andReturn(Collections.singletonList(new TopicPartitionInfo(
      0,
      brokerNode,
      Collections.singletonList(brokerNode),
      Collections.singletonList(brokerNode)
    )))
    expect(mockSparkContext.makeRDD(
      Seq(
        (
          taskAssignment,
          Seq(host)
        )
      )
    )).andReturn(realRDD)

    replay(
      mockSparkContext,
      mockAdminClient,
      mockDescribeClusterResult,
      mockDescribeLogDirsResult,
      mockDescribeTopicsResult,
      mockTopicDescription
    )

    kasparDriver.adminClient = mockAdminClient

    val createdIndices = kasparDriver.createIndex(mockSparkContext,
      topic,
      rowDeserializer,
      indexName,
      indexFunction
    ).collect()

    // assert file contents
    val indexContent = scala.io.Source.fromFile(indexFileName).mkString
    assert(indexContent == "someIndexVal")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    assert(createdIndices.length == 1)
    assert(createdIndices(0)._1 == host)
    assert(createdIndices(0)._2 == "partition: 0 segment: " + currentDirectory + "/" + segmentFileName)
    assert(createdIndices(0)._3 == true)

    // clean up
    new File(indexFileName).delete()
  }

}


