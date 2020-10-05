package kaspar.dataload.structure

class RawRow extends Serializable {

  private var rawVals:Seq[Any] = null

  def setRawVals(rawVals: Seq[Any]): Unit = {
    this.rawVals = rawVals
  }

  def getColumnVal(columnIndex: Int): Any = {
    if (rawVals != null && columnIndex < rawVals.length) return rawVals(columnIndex)
    ""
  }

  def getIntVal(columnIndex: Int): Integer = {
    if (getColumnVal(columnIndex).isInstanceOf[Int]) {
      rawVals(columnIndex).asInstanceOf[Integer]
    } else {
      throw new IllegalArgumentException("RequestedColumn is not an Int")
    }
  }

  def getStringVal(columnIndex: Int): String = {
    if (getColumnVal(columnIndex).isInstanceOf[String]) {
      rawVals(columnIndex).asInstanceOf[String]
    } else {
      throw new IllegalArgumentException("RequestedColumn is not a String")
    }
  }

  def getLongVal(columnIndex: Int): java.lang.Long = {
    if (getColumnVal(columnIndex).isInstanceOf[Long]) {
      rawVals(columnIndex).asInstanceOf[java.lang.Long]
    } else {
      throw new IllegalArgumentException("RequestedColumn is not a Long")
    }
  }

  def getDoubleVal(columnIndex: Int): java.lang.Double = {
    if (getColumnVal(columnIndex).isInstanceOf[Double]) {
      rawVals(columnIndex).asInstanceOf[java.lang.Double]
    } else {
      throw new IllegalArgumentException("RequestedColumn is not a Double")
    }
  }


}
