package kaspar.dataload.predicate

import scala.io.Source

object MinMaxPredicate {

  def buildGreaterThanSegmentPredicate(threshold: Int, columnIndex: Int) : (String, Int, String) => Boolean =  {
    (topicName: String, partition: Int, segmentFileName: String) => {
      val indexFileName = segmentFileName.dropRight(3) + "minMax.index"
      var shouldRead = true
      for (line <- Source.fromFile(indexFileName).getLines) {
        if(line.split(':')(0).toInt == columnIndex && line.split(':')(2).toInt < threshold) {
          shouldRead = false
        }
      }
      shouldRead
    }
  }
}
