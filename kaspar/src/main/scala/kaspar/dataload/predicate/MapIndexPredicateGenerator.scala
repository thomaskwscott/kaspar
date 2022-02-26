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
column -> value -> repeating interval ranges for value in segment, for example:
1:abcd:10:500:700:750:850:920 - column index 1, value abcd and 3 intervals 10-500, 700-750 and 850-920

Using the above a segment predicate can reduce the amount of segment it needs to read
 */
class MapIndexPredicateGenerator() extends PredicateGenerator with Serializable {
  val MAX_GAP: Int = 20000; // 10 MB - maximum gap between records in the interval to trigger closure of interval and start of a new one.

  override def getIndexName(): String = "map.index"

  override def getIndexFunction(columnsToIndex: Seq[(Int, ColumnType)]):
  Seq[PositionRawRow] => String = {
    records: Seq[PositionRawRow] => {
      val columnsMap = columnsToIndex.map(column => {
        val columnMap = mutable.Map[String, mutable.Buffer[(Int, Int)]]()
        records.foreach(positionRawRow => {
          // for now we will tostring as we don't care about types
          val columnVal = positionRawRow.rawRow.getStringVal(column._1)
          val currIndexList = columnMap.getOrElse(columnVal, mutable.Buffer())
          val currEntry = currIndexList.lastOption.getOrElse((-1, -1))

          if (currEntry._2 == -1 || (currEntry._2 + MAX_GAP < positionRawRow.position)) { //if its first match or if gap bigger than max
            currIndexList.append((positionRawRow.position, positionRawRow.position)) //  add new tuple to list with min,max set to current row position
          } else {
            currIndexList(currIndexList.length - 1) = (currEntry._1, positionRawRow.position) //otherwise - update last index tuple with new max position
          }
          columnMap.put(columnVal, currIndexList)
        })
        (column._1, columnMap.toMap)
      }).toMap
      serializeIndex(columnsMap)
    }
  }

  private def serializeIndex(index: Map[Int, Map[String, Seq[(Int, Int)]]]): String = {
    index.flatMap(columnEntry => {
      val columnIndex = columnEntry._1
      columnEntry._2.map(valueEntry => {
        val value = valueEntry._1
        val intervalRanges = valueEntry._2.flatten { case (a, b) => Seq(a, b) }
        (List(columnIndex, value) ++ intervalRanges).iterator.mkString(":")
      })
    }).mkString("\n")
  }

  override def segmentPredicateFromJson(jsonConfig: String):
  (Seq[File], String, Int, String) => Seq[(Int, Int)] = {
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
      var readRange = Seq((KasparDriver.DO_NOT_READ_SEGMENT, KasparDriver.DO_NOT_READ_SEGMENT))
      val indexFileName = segmentFileName.dropRight(3) + getIndexName()
      if (Files.exists(Paths.get(indexFileName))) {
        for (line <- Source.fromFile(indexFileName).getLines) {
          val indexVals = line.split(':')
          if (indexVals(0).toInt == columnIndex && indexVals(1).toString == value) {
            readRange = indexVals.drop(2).grouped(2).toStream.map(pair => (pair(0).toInt, pair(1).toInt)).toList
          }
        }
      } else {
        // index file does not exist so we should read everything
        readRange = Seq((KasparDriver.READ_WHOLE_SEGMENT, KasparDriver.READ_WHOLE_SEGMENT))
      }
      readRange
    }
  }

  override def rowPredicateFromJson(jsonConfig: String): RawRow => Boolean = {
    throw new IllegalStateException("Hash index")
  }
}