package kaspar.dataload

import java.io.{File, FilenameFilter, IOException}
import java.util.{Collections, Properties}

import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kaspar.dataload.structure.{Columnifier, RawRow}
import kaspar.dataload.metadata.{Location, TaskAssignment}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.Utils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


object TopicLoader {

  def getRawRows(sc: SparkContext, topicName: String,
                 clientProps: Properties, columnifier: Columnifier,
                 rowPredicates: Array[(String,Int,RawRow)=> Boolean] = Array(),
                 segmentPredicates: Array[(String,Int,String) => Boolean] = Array()) : RDD[RawRow] = {

    val adminClient: AdminClient = AdminClient.create(clientProps)

    // get datadirs for all brokers, these will form part of the task assignments
    val brokers = adminClient.describeCluster.nodes.get.asScala.map(n => new Integer(n.id)).asJavaCollection
    val brokerDirMappings = adminClient.describeLogDirs(brokers).all.get().asScala.map{case(k,v) => k -> v.keySet.asScala.toSeq}

    val descriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all.get
    val taskAssignments = descriptions.get(topicName).partitions.asScala.map(partition => {
      val locations = partition.isr().asScala.map(i => Location(i.host(),i.id(),brokerDirMappings(i.id())))
      (TaskAssignment(partition.partition(),locations) -> locations.map(i => i.host))
    })


    val rawData: RDD[RawRow] = sc.makeRDD(taskAssignments).flatMap((taskAssignment: TaskAssignment) => {
      val hosts = taskAssignment.locations.map(l => l.host)
      val actualHost = java.net.InetAddress.getLocalHost().getHostName()
      if (!hosts.contains(actualHost)) throw new RuntimeException("Ignore this, Spark scheduled this task " +
        "on the wrong broker. Expected: " + hosts.mkString(",") + " actual: " + actualHost + ". \n" + "You should " +
        "have blacklisting configurations that mean this will be rescheduled on a different node\n")
      val dataDirs = taskAssignment.locations.filter(l => l.host == actualHost)(0).dataDirs
      getFileRecords(topicName, taskAssignment.partitionId, columnifier, dataDirs, rowPredicates, segmentPredicates).iterator
    })
    rawData
  }

  @throws[IOException]
  private def getFileRecords(topicName: String, partition: Int,
                             columnifier: Columnifier,
                             dataDirs: Seq[String],
                             rowPredicates: Seq[(String,Int,RawRow) => Boolean],
                             segmentPredicates: Seq[(String,Int,String) => Boolean]): Seq[RawRow] = {

    val partitionFiles = dataDirs.flatMap(dataDir => {
      val foundFiles = new File(dataDir + "/" + topicName + "-" + partition).listFiles(
        new FilenameFilter {
          override def accept(file: File, s: String): Boolean = s.toLowerCase().endsWith(".log")
        }
      )
      if(foundFiles==null) {
        Seq.empty[File]
      } else {
        foundFiles
      }
    })

    partitionFiles.flatMap(segmentFile => {

      // check for segment predicate
      if(segmentPredicates.forall(predicate =>
        predicate(topicName,partition,segmentFile.getPath())
      )) {

        val records: FileRecords = FileRecords.open(segmentFile)
        val decoder: Decoder[String] = new StringDecoder(new VerifiableProperties)

        records.batches.asScala.flatMap(batch => {
          batch.asScala.map(record => {
            val newRow: RawRow = new RawRow()
            val rawValue: String = partition + "-" + record.offset + "," + record.timestamp + "," +
              decoder.fromBytes(Utils.readBytes(record.value))
            newRow.setRawVals(columnifier.toColumns(rawValue))
            newRow
          }).filter(newRow => {
            rowPredicates.forall(predicate => {
              predicate(topicName,partition,newRow)
            })
          })
        })
      } else {
        Array[RawRow]()
      }
    })
  }
}