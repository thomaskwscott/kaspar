package kaspar.dataload.predicate

import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.structure.{PositionRawRow, RawRow}

import java.io.File

trait PredicateGenerator {

  def getIndexName(): String

  def getIndexFunction(columnsToIndex: Seq[(Int, ColumnType)]): Seq[PositionRawRow] => String

  def segmentPredicateFromJson(jsonConfig: String): (Seq[File], String, Int, String) => Seq[(Int, Int)]

  def rowPredicateFromJson(jsonConfig: String): (RawRow) => Boolean
}

object PredicateType extends Enumeration {

  type PredicateType = Value

  val SEGMENT = Value("Segment")
  val ROW = Value("Row")

}

