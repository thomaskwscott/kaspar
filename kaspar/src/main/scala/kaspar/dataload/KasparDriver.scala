package kaspar.dataload

import kaspar.dataload.KasparDriver.READ_WHOLE_SEGMENT

import java.io.{File, FilenameFilter, IOException}
import java.util.{Collections, Properties}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.metadata.{ColumnType, Location, TaskAssignment}
import kaspar.dataload.structure.{PositionRawRow, RawRow, RowDeserializer}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch
import org.apache.kafka.common.record.{AbstractRecords, FileRecords, RecordBatch}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext}

import scala.collection.JavaConverters._

class KasparDriver  (clientProps: Properties) extends Serializable {

  // visible for testing
  @transient protected[dataload] var adminClient: AdminClient = null

  def registerTable(sqlContext: SQLContext,
                    rawRdd: RDD[RawRow],
                    tableName: String,
                    columnMappings: Seq[(String,ColumnType)]) = {

    val enrichedMappings = Array(
      ("_offset", ColumnType.LONG),
      ("_partition", ColumnType.INTEGER),
      ("_timestamp", ColumnType.LONG)
    ) ++ columnMappings

    val rows = rawRdd.map(rawRow => RowFactory.create(

      Array.tabulate(enrichedMappings.length){ i:Int => {
        enrichedMappings(i)._2 match  {
          case ColumnType.LONG => rawRow.getLongVal(i)
          case ColumnType.STRING => rawRow.getStringVal(i)
          case ColumnType.DOUBLE => rawRow.getDoubleVal(i)
          case ColumnType.INTEGER => rawRow.getIntVal(i)
          case _ => rawRow.getStringVal(i)
      } }}:_*
    ))

    val cols = enrichedMappings.map( i => {
      val dataType = i._2 match {
        case ColumnType.LONG => DataTypes.LongType
        case ColumnType.STRING => DataTypes.StringType
        case ColumnType.DOUBLE => DataTypes.DoubleType
        case ColumnType.INTEGER => DataTypes.IntegerType
        case _ => DataTypes.StringType
      }
      StructField(i._1,dataType,false,Metadata.empty)
    })

    val schema = StructType(cols)

    val df = sqlContext.createDataFrame(rows,schema)
    df.createOrReplaceTempView(tableName)
  }

  def getRows(sparkContext: SparkContext,
              topicName: String,
              rowDeserializer: RowDeserializer,
              rowPredicates: Array[(RawRow)=> Boolean] = Array(),
              segmentPredicates: Array[(Seq[File],String,Int,String) => (Int,Int)] = Array()) : RDD[RawRow] = {

    val taskAssignments = getTaskAssignments(topicName)

    val rawData: RDD[RawRow] = sparkContext.makeRDD(taskAssignments).flatMap((taskAssignment: TaskAssignment) => {
      val hosts = taskAssignment.locations.map(l => l.host)
      val actualHost = validHost(hosts, java.net.InetAddress.getLocalHost().getHostName())
      if (actualHost.isEmpty) throw new RuntimeException("Ignore this, Spark scheduled this task " +
        "on the wrong broker. Expected: " + hosts.mkString(",") + " actual: " + actualHost + ". \n" + "You should " +
        "have blacklisting configurations that mean this will be rescheduled on a different node\n")
      val dataDirs = taskAssignment.locations.filter(l => l.host == actualHost)(0).dataDirs
      getRecords(topicName, taskAssignment.partitionId, rowDeserializer, dataDirs, rowPredicates, segmentPredicates).iterator
    })
    rawData
  }

  def validHost(hosts: Seq[String], actualHost: String): String = {
    val filteredHosts = hosts.filter(host => host.startsWith(actualHost))
    if (filteredHosts.nonEmpty) {
      filteredHosts.head
    } else {
      ""
    }
  }

  def createIndex(sparkContext: SparkContext,
                  topicName: String,
                  rowDeserializer: RowDeserializer,
                  indexName: String,
                  indexFunction: Seq[PositionRawRow] => String) : RDD[(String,String,Boolean)] = {

    val taskAssignments = getTaskAssignments(topicName)

    // task assignments are one task for each partition but we need one task for each replica
    // this makes index creation EXPENSIVE!
    val perReplicaAssignments = taskAssignments.flatMap(task  => {
      val assignment = task._1
      assignment.locations.map(location => (TaskAssignment(assignment.partitionId,Seq(location)) -> Seq(location.host)))
    })

    val indexesCreated = sparkContext.makeRDD(perReplicaAssignments).flatMap((taskAssignment: TaskAssignment) => {
      val hosts = taskAssignment.locations.map(l => l.host)
      val actualHost = java.net.InetAddress.getLocalHost().getHostName()
      if (!hosts.contains(actualHost)) {
        Seq((actualHost,"partition: " + taskAssignment.partitionId, false))
      } else {
        val dataDirs = taskAssignment.locations.filter(l => l.host == actualHost)(0).dataDirs
        // remove the latest segment as we will never index this because it could be being written to
        val segments = getSegments(dataDirs, topicName, taskAssignment.partitionId).sorted.dropRight(1)
        segments.map(segment => {
          // the segment has data and there isn't a pre-existing index
          val indexPath = Paths.get(segment.getAbsolutePath().dropRight(3) + indexName)
          if( segment.length() > 0 && !Files.exists(indexPath)) {
            val segmentRecords = getSegmentRecords(segment, taskAssignment.partitionId, READ_WHOLE_SEGMENT, READ_WHOLE_SEGMENT, rowDeserializer, Seq.empty)
            // indexFunction returns  what to write to the index file
            val indexData = indexFunction(segmentRecords)
            Files.write(indexPath, indexData.getBytes(StandardCharsets.UTF_8))
          }
          (actualHost, "partition: " + taskAssignment.partitionId + " segment: " + segment.getAbsolutePath(), true)
        })
      }
    })
    indexesCreated
  }

  private def getTaskAssignments(topicName: String): Seq[(TaskAssignment, Seq[String])] = {
    if (adminClient == null) {
      adminClient = AdminClient.create(clientProps)
    }

    // get datadirs for all brokers, these will form part of the task assignments
    val brokers = adminClient.describeCluster.nodes.get.asScala.map(n => new Integer(n.id)).asJavaCollection
    val brokerDirMappings = adminClient.describeLogDirs(brokers).all.get().asScala.map{case(k,v) => k -> v.keySet.asScala.toSeq}

    val descriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all.get
    val taskAssignments = descriptions.get(topicName).partitions.asScala.map(partition => {
      val locations = partition.isr().asScala.map(i => Location(i.host(),i.id(),brokerDirMappings(i.id()))).toSeq
      (TaskAssignment(partition.partition(), locations) -> locations.map(i => i.host))
    })
    taskAssignments.toSeq
  }

  @throws[IOException]
  private def getRecords(topicName: String, partition: Int,
                         rowDeserializer: RowDeserializer,
                         dataDirs: Seq[String],
                         rowPredicates: Seq[(RawRow) => Boolean],
                         segmentPredicates: Seq[(Seq[File],String,Int,String) => (Int, Int)]): Seq[RawRow] = {

    val partitionFiles = getSegments(dataDirs,topicName,partition)

    partitionFiles.flatMap(segmentFile => {


      val segmentBounds = if (segmentPredicates.isEmpty)
        (KasparDriver.READ_WHOLE_SEGMENT,  KasparDriver.READ_WHOLE_SEGMENT)
      else
        getSegmentBounds(topicName, partition, partitionFiles, segmentFile, segmentPredicates)

      // check for segment predicate
      if(segmentBounds._1 == KasparDriver.DO_NOT_READ_SEGMENT) {
        Array[RawRow]()
      } else {
        getSegmentRecords(segmentFile,partition, segmentBounds._1, segmentBounds._2, rowDeserializer,rowPredicates)
          .map(record => record.rawRow)
      }
    })
  }

  private def getSegmentBounds(topicName: String,
                               partition: Int,
                               partitionFiles: Seq[File],
                               segmentFile: File,
                               segmentPredicates: Seq[(Seq[File],String,Int,String) => (Int, Int)]): (Int,Int) = {
    segmentPredicates.map(predicate =>
      predicate(partitionFiles, topicName, partition, segmentFile.getPath())
    ).reduce((left,  right) => (left._1 min right._1, left._2 min right._2))
  }

  private def getSegmentRecords(segmentFile: File,
                                partition: Int,
                                startPosition: Int,
                                endPosition: Int,
                                rowDeserializer: RowDeserializer,
                                rowPredicates: Seq[RawRow => Boolean]): Seq[PositionRawRow] = {
    val records: FileRecords = FileRecords.open(segmentFile)
    rowsFromRecords(records,partition,startPosition, endPosition, rowDeserializer,rowPredicates)
  }

  private def getSegments(dataDirs: Seq[String], topicName: String, partition: Int) =  {
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
    partitionFiles
  }

  private def rowsFromRecords(records: AbstractRecords,
                              partition: Int,
                              startPosition: Int,
                              endPosition: Int,
                              rowDeserializer: RowDeserializer,
                              rowPredicates: Seq[(RawRow) => Boolean]): Seq[PositionRawRow] = {
    // we're reading the whole records
    if (startPosition == READ_WHOLE_SEGMENT || !records.isInstanceOf[FileRecords]) {
      records.batches.asScala.flatMap(batch => {
        batchToRawRows(partition, batch, rowDeserializer, rowPredicates)
      }).toSeq
    } else {
      // we're reading a subset of records
      val batchIter = records.asInstanceOf[FileRecords].batchesFrom(startPosition).asScala.iterator
      batchIter.takeWhile(batch => batch.position() == endPosition).flatMap(batch => {
        batchToRawRows(partition, batch, rowDeserializer, rowPredicates)
      }
      ).toSeq
    }
  }

  private def batchToRawRows(partition: Int,
                             batch: RecordBatch,
                             rowDeserializer: RowDeserializer,
                             rowPredicates: Seq[(RawRow) => Boolean]): Seq[PositionRawRow] = {
    val position = if (batch.isInstanceOf[FileChannelRecordBatch])
        batch.asInstanceOf[FileChannelRecordBatch].position()
    else
      -1
    batch.asScala.map(record => {
      val newRow: RawRow = new RawRow(rowDeserializer.toColumns(partition, record))
      PositionRawRow(position,newRow)
    }).filter(newRow => {
      rowPredicates.forall(predicate => {
        predicate(newRow.rawRow)
      })
    })
  }.toSeq
}

object KasparDriver {
  def builder() : KasparDriverBuilder = KasparDriverBuilder()

  // flags for predicate reads
  final val DO_NOT_READ_SEGMENT =  -2
  final val READ_WHOLE_SEGMENT = -1
}

case class KasparDriverBuilder (
    clientProperties: Properties = null){

  def withClientProperties(properties: Properties): KasparDriverBuilder = copy(clientProperties = properties)

  def build() : KasparDriver = new KasparDriver(
    clientProperties
  )
}