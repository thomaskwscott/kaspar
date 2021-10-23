package kaspar.dataload.predicate
import com.jayway.jsonpath.JsonPath
import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.structure.RawRow

import java.io.File
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

  class OffsetPredicateGenerator() extends PredicateGenerator with Serializable {
  override def getIndexName(): String = "offset"

  override def getIndexFunction(columnsToIndex: Seq[(Int, ColumnType)]): Seq[RawRow] => String = {
    throw new IllegalStateException("Offset index is pre-existing and should not be recreated.")
  }

  override def segmentPredicateFromJson(jsonConfig: String):
  (Seq[File], String, Int, String) => Boolean = {

    val config = getConfigFromJson(jsonConfig)
    val predicateType = config._1
    val partitionThresholds = config._2
    if (predicateType == "GreaterThan") {
      (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => {

        if (!partitionThresholds.contains(partition)) {
          // if you don't provide a threshold for this partition we will read segments regardless
          true
        } else {
          val offsetSegmentsMap = partitionFiles.map(file => file.getName.dropRight(4).toInt -> file.getPath).toMap
          val offsetSegments = offsetSegmentsMap.keys.toArray.sorted

          var shouldRead = false

          // if there is only 1 segment or this is the last segment we can bail out here as we will always read it
          if (offsetSegments.length == 1 || segmentFileName == offsetSegmentsMap(offsetSegments.last)) {
            true
          } else {
            // find the segment which contains the crossing offset.
            val threshold = partitionThresholds(partition)
            breakable {
              for (index <- 1 until offsetSegments.length - 1) {
                if (offsetSegments(index) > threshold) {
                  // this segment for sure should be read
                  if (segmentFileName == offsetSegmentsMap(offsetSegments(index))) {
                    shouldRead = true
                    break
                  }
                  // if we are more than one ahead of the threshold we read the predecessor segment too
                  if (offsetSegments(index) > threshold + 1L && segmentFileName == offsetSegmentsMap(offsetSegments(index - 1))) {
                    shouldRead = true
                    break
                  }
                }
              }
            }
            shouldRead
          }
        }
      }
    } else {
      (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => false
    }
  }

  private def getConfigFromJson(jsonConfig: String): (String,Map[Int,Long]) = {
    /*
    {
      "predicateType": "GreaterThan"
      "partitionThresholds" : [
        { "partition": 0, "threshold": 10 },
        { "partition": 1, "threshold": 20 }
      ]
    }
    */
    val jsonConfigObj = JsonPath.parse(jsonConfig)
    val predicateType = jsonConfigObj.read[String]("$.predicateType")
    val thresholdCount = jsonConfigObj.read[Int]("$.partitionThresholds.length()")
    val partitionThresholds = mutable.Map[Int, Long]()
    for( thresholdIndex <- 0 to thresholdCount-1)  {
      val partition = jsonConfigObj.read[Int]("$.partitionThresholds[" + thresholdIndex + "].partition")
      val threshold = jsonConfigObj.read[Int]("$.partitionThresholds[" + thresholdIndex + "].threshold")
      partitionThresholds(partition) = threshold;
    }
    (predicateType, partitionThresholds.toMap)
  }

  override def rowPredicateFromJson(jsonConfig: String): RawRow => Boolean = {
    val config = getConfigFromJson(jsonConfig)
    val predicateType = config._1
    val partitionThresholds = config._2
    if (predicateType == "GreaterThan") {
      (rawRow: RawRow) => {
        val offset = rawRow.getLongVal(0)
        val partition = rawRow.getIntVal(1)
        if (!partitionThresholds.contains(partition)) {
          true
        } else {
          val threshold = partitionThresholds(partition)
          if (offset > threshold) {
            true
          } else {
            false
          }
        }
      }
    } else {
      (rawRow: RawRow) => true
    }
  }
}
