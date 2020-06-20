package kaspar.dataload.structure

class CsvColumnifier(val delimiter: String = ",") extends Columnifier with Serializable {

  override def toColumns(raw: String): scala.Seq[String] = {
    raw.split(delimiter)
  }
}
