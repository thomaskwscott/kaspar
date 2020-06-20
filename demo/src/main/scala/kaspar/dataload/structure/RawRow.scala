package kaspar.dataload.structure

class RawRow extends Serializable {

  private var rawVals:Seq[String] = null

  def setRawVals(rawVals: Seq[String]): Unit = {
    this.rawVals = rawVals
  }

  def getColumnVal(columnIndex: Int): String = {
    if (rawVals != null && columnIndex < rawVals.length) return rawVals(columnIndex)
    ""
  }

}
