package kaspar.dataload.structure

trait Columnifier {
  def toColumns(raw: String):Seq[String]
}
