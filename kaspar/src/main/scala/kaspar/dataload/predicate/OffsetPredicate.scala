package kaspar.dataload.predicate

import java.io.File

import kaspar.dataload.structure.RawRow

import scala.util.control.Breaks._

object OffsetPredicate {

  val offsetColumn = 0

  def buildGreaterThanSegmentPredicate(partitionThresholds: Map[Int,Long]) : (Seq[File],String, Int, String) => Boolean =  {
    (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => {

      if (!partitionThresholds.contains(partition)) {
        // if you don't provide a threshold for this partition we will read segments regardless
        true
      } else {
        val offsetSegmentsMap = partitionFiles.map(file => file.getName.dropRight(4).toInt -> file.getPath).toMap
        val offsetSegments = offsetSegmentsMap.keys.toArray.sorted
        // find the segment which contains the crossing offset.
        var shouldRead = false
        val threshold = partitionThresholds(partition)
        breakable { for (index <- 1 until offsetSegments.length)
          {
            if (offsetSegments(index) > threshold) {
              // this segment for sure should be read
              if (segmentFileName == offsetSegmentsMap(offsetSegments(index))) {
                shouldRead = true
                break
              }
              // if we are more than one ahead of the threshold we read the predecessor segment too
              if(offsetSegments(index) > threshold + 1L && segmentFileName == offsetSegmentsMap(offsetSegments(index-1))) {
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

  def buildGreaterThanRowPredicate(partitionThresholds: Map[Int,Long]): (RawRow) => Boolean = {
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
  }
}
