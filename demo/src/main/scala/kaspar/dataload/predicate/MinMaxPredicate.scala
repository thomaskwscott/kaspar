package kaspar.dataload.predicate

import scala.io.Source

object MinMaxPredicate {

  def buildGreaterThanEqualSegmentPredicate(threshold: Int, columnIndex: Int) : (String) => Boolean =  {
    (segmentFileName: String) => {
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
