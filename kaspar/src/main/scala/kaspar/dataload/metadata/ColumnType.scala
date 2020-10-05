package kaspar.dataload.metadata

object ColumnType extends Enumeration {

  type ColumnType = Value

  val INTEGER = Value("Integer")
  val DOUBLE = Value("Double")
  val STRING = Value("String")
  val LONG = Value("Long")

}
