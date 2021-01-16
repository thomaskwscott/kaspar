package kaspar.dataload

import java.io.{File, FilenameFilter, IOException}
import java.nio.ByteBuffer
import java.util.{Collections, Properties}
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.metadata.{ColumnType, Location, TaskAssignment}
import kaspar.dataload.structure.{Columnifier, RawRow}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.record.{AbstractRecords, FileRecords, MemoryRecords}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext}

import scala.collection.JavaConverters._

object TopicLoader {

  def registerTableFromRdd(sqlContext: SQLContext,rawRdd: RDD[RawRow],tableName: String, columnMappings: Seq[(String,ColumnType)]) = {

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

  def getRawRowsFromKafka(sc: SparkContext, topicName: String,
                          clientProps: Properties, columnifier: Columnifier,
                          rowPredicates: Array[(RawRow)=> Boolean] = Array(),
                          segmentPredicates: Array[(Seq[File],String,Int,String) => Boolean] = Array()) : RDD[RawRow] = {

    val taskAssignments = getTaskAssignments(clientProps,topicName)

    val rawData: RDD[RawRow] = sc.makeRDD(taskAssignments).flatMap((taskAssignment: TaskAssignment) => {
      val hosts = taskAssignment.locations.map(l => l.host)
      val actualHost = java.net.InetAddress.getLocalHost().getHostName()
      if (!hosts.contains(actualHost)) throw new RuntimeException("Ignore this, Spark scheduled this task " +
        "on the wrong broker. Expected: " + hosts.mkString(",") + " actual: " + actualHost + ". \n" + "You should " +
        "have blacklisting configurations that mean this will be rescheduled on a different node\n")
      val dataDirs = taskAssignment.locations.filter(l => l.host == actualHost)(0).dataDirs
      getRecords(topicName, taskAssignment.partitionId, columnifier, dataDirs, rowPredicates, segmentPredicates).iterator
    })
    rawData
  }

  def createIndex(sc: SparkContext, topicName: String,
                          clientProps: Properties, columnifier: Columnifier,
                          indexName: String,
                          indexFunction: Seq[RawRow] => String) : RDD[(String,String,Boolean)] = {

    val taskAssignments = getTaskAssignments(clientProps,topicName)

    // task assignments are one task for each partition but we need one task for each replica
    // this makes index creation EXPENSIVE!
    val perReplicaAssignments = taskAssignments.flatMap(task  => {
      val assignment = task._1
      assignment.locations.map(location => (TaskAssignment(assignment.partitionId,Seq(location)) -> Seq(location.host)))
    })

    val indexesCreated = sc.makeRDD(perReplicaAssignments).flatMap((taskAssignment: TaskAssignment) => {
      val hosts = taskAssignment.locations.map(l => l.host)
      val actualHost = java.net.InetAddress.getLocalHost().getHostName()
      if (!hosts.contains(actualHost)) {
        Seq((actualHost,"partition: " + taskAssignment.partitionId, false))
      } else {
        val dataDirs = taskAssignment.locations.filter(l => l.host == actualHost)(0).dataDirs
        val segments = getSegments(dataDirs, topicName, taskAssignment.partitionId)
        segments.map(segment => {
          if( segment.length() > 0 ) {
            val segmentRecords = getSegmentRecords(segment, taskAssignment.partitionId, columnifier, Seq.empty)
            // indexfunction returns  what to write to the index file
            val indexData = indexFunction(segmentRecords)
            Files.write(Paths.get(segment.getAbsolutePath().dropRight(3) + indexName), indexData.getBytes(StandardCharsets.UTF_8))
          }
          (actualHost, "partition: " + taskAssignment.partitionId + " segment: " + segment.getAbsolutePath(), true)
        })
      }
    })
    indexesCreated
  }
  
  private def getTaskAssignments(clientProps: Properties, topicName: String): Seq[(TaskAssignment, Seq[String])] = {
    val adminClient: AdminClient = AdminClient.create(clientProps)

    // get datadirs for all brokers, these will form part of the task assignments
    val brokers = adminClient.describeCluster.nodes.get.asScala.map(n => new Integer(n.id)).asJavaCollection
    val brokerDirMappings = adminClient.describeLogDirs(brokers).all.get().asScala.map{case(k,v) => k -> v.keySet.asScala.toSeq}

    val descriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all.get
    val taskAssignments = descriptions.get(topicName).partitions.asScala.map(partition => {
      val locations = partition.isr().asScala.map(i => Location(i.host(),i.id(),brokerDirMappings(i.id())))
      (TaskAssignment(partition.partition(),locations) -> locations.map(i => i.host))
    })
    taskAssignments
  }

  @throws[IOException]
  private def getRecords(topicName: String, partition: Int,
                             columnifier: Columnifier,
                             dataDirs: Seq[String],
                             rowPredicates: Seq[(RawRow) => Boolean],
                             segmentPredicates: Seq[(Seq[File],String,Int,String) => Boolean]): Seq[RawRow] = {

    val partitionFiles = getSegments(dataDirs,topicName,partition)

    partitionFiles.flatMap(segmentFile => {

      // check for segment predicate
      if(segmentPredicates.forall(predicate =>
        predicate(partitionFiles,topicName,partition,segmentFile.getPath())
      )) {
        getSegmentRecords(segmentFile,partition,columnifier,rowPredicates)
      } else {
        Array[RawRow]()
      }
    })
  }

  private def getSegmentRecords(segmentFile: File, partition: Int, columnifier: Columnifier, rowPredicates: Seq[RawRow => Boolean]): Seq[RawRow] = {
    val records: FileRecords = FileRecords.open(segmentFile)
    rowsFromRecords(records,partition,columnifier,rowPredicates)
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

  @throws[IOException]
  def getRawRowsFromS3(sc: SparkContext, columnifier: Columnifier, accessKey: String, secret: String, region: String,
                           bucketName: String, s3objects: Seq[String],
                           rowPredicates: Seq[(RawRow) => Boolean]): RDD[RawRow] = {

    val rawData: RDD[RawRow] = sc.makeRDD(s3objects).flatMap(objectName => {

      val awsCreds = new BasicAWSCredentials(accessKey, secret)
      val s3Client = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(
        new AWSStaticCredentialsProvider(awsCreds)).build()

      val fullObject = s3Client.getObject(new GetObjectRequest(bucketName, objectName))

      val bytes = fullObject.getObjectContent().readAllBytes()
      val buffer = ByteBuffer.wrap(bytes)

      val records = MemoryRecords.readableRecords(buffer)
      rowsFromRecords(records,-1,columnifier,rowPredicates)

    })
    rawData
  }

  private def rowsFromRecords(records: AbstractRecords, partition: Int, columnifier: Columnifier,rowPredicates: Seq[(RawRow) => Boolean]): Seq[RawRow] = {
    records.batches.asScala.flatMap(batch => {
      batch.asScala.map(record => {
        val newRow: RawRow = new RawRow()
        newRow.setRawVals(columnifier.toColumns(partition, record))
        newRow
      }).filter(newRow => {
        rowPredicates.forall(predicate => {
          predicate(newRow)
        })
      })
    }).toSeq
  }
}