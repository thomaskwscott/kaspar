package kaspar.dataload

import java.io.{File, FileInputStream, FilenameFilter, IOException}
import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.admin.{AdminClient, TopicDescription}
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.Utils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import kafka.serializer.Decoder
import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties

import scala.collection.JavaConversions._
import scala.collection.mutable

import kaspar.dataload.structure.RawRow
import kaspar.dataload.structure.Columnifier

object TopicLoader {

  var dataDir = "/var/lib/kafka/data"
  var serverPropertiesLocation = "/etc/kafka/kafka.properties"

  def setDataDir(dataDir: String): Unit = {
    this.dataDir = dataDir
  }

  def setServerProperties(serverPropertiesLocation: String): Unit = {
    this.serverPropertiesLocation = serverPropertiesLocation
  }

  def getRawRows(sc: SparkContext, dataDir: String, serverProperties: String, topicName: String,
                 clientProps: Properties, columnifier: Columnifier,
                 rowPredicates: Array[(RawRow)=> Boolean] = Array(),
                 segmentPredicates: Array[(String) => Boolean] = Array()) : RDD[RawRow] = {
    setDataDir(dataDir)
    setServerProperties(serverProperties)
    val adminClient: AdminClient = AdminClient.create(clientProps)

    val idHostnameMappings = getIdHostnameMappings(adminClient)

    val taskAssigments = mutable.ArrayBuffer[(String, Seq[String])]()
    try {
      val descriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all.get
      val brokerLeaderMappings = mutable.Map[Int, mutable.ArrayBuffer[Int]]()
      for (partition <- descriptions.get(topicName).partitions) {
        val leaderBroker: Int = partition.leader.id
        val partitionId: Int = partition.partition

        if (brokerLeaderMappings.contains(leaderBroker)) {
          brokerLeaderMappings(leaderBroker) += partitionId
        }else {
          brokerLeaderMappings += (leaderBroker -> mutable.ArrayBuffer[Int](partitionId))
        }
      }
      brokerLeaderMappings.foreach { case(k , v) => {
        taskAssigments += (k + ":" + v.mkString(",") -> Seq(idHostnameMappings(k)))
      }}
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.out.println("bad thing happened")
    }

    val rawData: RDD[RawRow] = sc.makeRDD(taskAssigments).flatMap((i: String) => {
        val expectedBrokerId: String = i.split(":")(0)
        val brokerHostedPartitions: Array[String] = i.split(":")(1).split(",")
        val actualbrokerId: String = getBrokerId
        if (!(expectedBrokerId == actualbrokerId)) throw new RuntimeException("Ignore this, Spark scheduled this task " +
          "on the wrong broker. Expected: " + expectedBrokerId + " actual: " + actualbrokerId + ". \n" + "You should " +
          "have blacklisting configurations that mean this will be rescheduled on a different node\n")
        getFileRecords(topicName, brokerHostedPartitions, columnifier, rowPredicates, segmentPredicates).iterator
    })
    rawData
  }

  private def getBrokerId():String = {
    val props = new Properties
    try props.load(new FileInputStream(serverPropertiesLocation))
    props.getProperty("broker.id")
  }

  private def getIdHostnameMappings(adminClient:AdminClient):Map[Int,String] = {
    val describeClisterResult = adminClient.describeCluster
    describeClisterResult.nodes.get.map(i=> (i.id() -> i.host())).toMap
  }

  @throws[IOException]
  private def getFileRecords(topicName: String, partitions: Seq[String], columnifier: Columnifier,
                             rowPredicates: Seq[RawRow => Boolean],
                             segmentPredicates: Seq[String => Boolean]): Seq[RawRow] = {
    partitions.flatMap(partition => {
      val partitionFiles = new File(dataDir + "/" + topicName + "-" + partition).listFiles(
        new FilenameFilter {
          override def accept(file: File, s: String): Boolean = s.toLowerCase().endsWith(".log")
        }
      )
      partitionFiles.flatMap(segmentFile => {

        // check for segment predicate
        if(segmentPredicates.forall(predicate =>
          predicate(segmentFile.getPath())
        )) {

          val records: FileRecords = FileRecords.open(segmentFile)
          val decoder: Decoder[String] = new StringDecoder(new VerifiableProperties)

          records.batches.flatMap(batch => {
            batch.map(record => {
              val newRow: RawRow = new RawRow()
              val rawValue: String = partition + "-" + record.offset + "," + record.timestamp + "," +
                decoder.fromBytes(Utils.readBytes(record.value))
              newRow.setRawVals(columnifier.toColumns(rawValue))
              newRow
            }).filter(newRow => {
              rowPredicates.forall(predicate => {
                predicate(newRow)
              })
            })
          })
        } else {
          Array[RawRow]()
        }
      })
    })
  }
}
