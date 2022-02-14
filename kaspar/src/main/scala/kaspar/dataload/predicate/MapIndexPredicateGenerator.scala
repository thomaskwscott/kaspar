package kaspar.dataload.predicate

import com.jayway.jsonpath.JsonPath
import kaspar.dataload.KasparDriver
import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.structure.{PositionRawRow, RawRow}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source

/*
This creates an index file that represents:

column -> value -> earliest position in segment for value -> latest position in segment for value

Using the above a segment predicate can reduce the amount of segment it needs to read
 */
class MapIndexPredicateGenerator() extends PredicateGenerator with Serializable {

  override def getIndexName(): String = "map.index"

  override def getIndexFunction(columnsToIndex: Seq[(Int, ColumnType)]):
  Seq[PositionRawRow] => String = {
    records: Seq[PositionRawRow] => {
      val columnsMap = columnsToIndex.map(column => {
        val columnMap = mutable.Map[String, (Int,Int)]()
        records.foreach(positionRawRow => {
          // for now we will tostring as we don't care about types
          val columnVal = positionRawRow.rawRow.getStringVal(column._1)
          val currEntry = columnMap.get(columnVal).getOrElse((positionRawRow.position, positionRawRow.position))
          // we read sequentially so don't need to worry about the min case
          if (currEntry._2 < positionRawRow.position) {
            columnMap.put(columnVal,(currEntry._1,positionRawRow.position))
          } else {
            columnMap.put(columnVal, currEntry)
          }
        })
        (column._1, columnMap.toMap)
      }).toMap
      serializeIndex(columnsMap)
    }
  }

  private def serializeIndex(index: Map[Int, Map[String, (Int,Int)]]) : String = {
    index.flatMap(columnEntry => {
      val columnIndex = columnEntry._1
      columnEntry._2.map(valueEntry => {
        val value = valueEntry._1
        val startPosition = valueEntry._2._1
        val endPosition = valueEntry._2._2
        (columnIndex, value, startPosition, endPosition).productIterator.mkString(":")
      })
    }).mkString("\n")
  }

  override def segmentPredicateFromJson(jsonConfig: String):
  (Seq[File], String, Int, String) => (Int, Int) = {
    /*
      json structure for map predicate:
      {
        "value" : "ABCD",
        "columnIndex" : 1
      }
    */
    val jsonConfigObj = JsonPath.parse(jsonConfig)
    val value = jsonConfigObj.read[String]("$.value")
    val columnIndex = jsonConfigObj.read[Int]("$.columnIndex")

    (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => {
      // default position is toread nothing
      var readRange =(KasparDriver.DO_NOT_READ_SEGMENT,KasparDriver.DO_NOT_READ_SEGMENT)
      val indexFileName = segmentFileName.dropRight(3) + getIndexName()
      if( Files.exists(Paths.get(indexFileName))) {
        for (line <- Source.fromFile(indexFileName).getLines) {
          val indexVals = line.split(':')
          if (indexVals(0).toInt == columnIndex && indexVals(1).toString == value) {
            readRange = (indexVals(2).toInt,indexVals(3).toInt)
          }
        }
      } else {
        // index file does not exist so we should read everything
        readRange = (KasparDriver.READ_WHOLE_SEGMENT,KasparDriver.READ_WHOLE_SEGMENT)
      }
      readRange
    }
  }

  override def rowPredicateFromJson(jsonConfig: String): RawRow => Boolean = {
    throw new IllegalStateException("Hash index")
  }
}