package kaspar.dataload.predicate

import com.jayway.jsonpath.JsonPath
import kaspar.dataload.KasparDriver
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.structure.{PositionRawRow, RawRow}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source

class MinMaxPredicateGenerator() extends PredicateGenerator with Serializable {

  override def getIndexName(): String = "minMax.index"

  override def getIndexFunction(columnsToIndex: Seq[(Int, ColumnType)]):
  Seq[PositionRawRow] => String = {
    records: Seq[PositionRawRow] => {
      columnsToIndex.map(column => {
        val columnRecords = records.map(positionRawRow => {
          val rawRow = positionRawRow.rawRow
          column._2 match {
            case ColumnType.LONG => rawRow.getLongVal(column._1)
            case ColumnType.DOUBLE => rawRow.getDoubleVal(column._1)
            case ColumnType.INTEGER => rawRow.getIntVal(column._1)
            case _ => rawRow.getStringVal(column._1)
          }
        })
        if (!columnRecords.isEmpty) {
          column._2 match {
            case ColumnType.LONG => column._1.toString + ":" + columnRecords.map(_.asInstanceOf[Long]).min + ":" + columnRecords.map(_.asInstanceOf[Long]).max
            case ColumnType.DOUBLE => column._1.toString + ":" + columnRecords.map(_.asInstanceOf[Double]).min + ":" + columnRecords.map(_.asInstanceOf[Double]).max
            case ColumnType.INTEGER => column._1.toString + ":" + columnRecords.map(_.asInstanceOf[Int]).min + ":" + columnRecords.map(_.asInstanceOf[Int]).max
            case _ => column._1.toString + ":" + columnRecords.map(_.toString).min + ":" + columnRecords.map(_.toString).max
          }
        }
      }).mkString("\n")
    }
  }

  override def segmentPredicateFromJson(jsonConfig: String):
  (Seq[File], String, Int, String) => Seq[(Int, Int)] = {
    /*
      json structure for greater than predicate:
      {
        "predicateType" : "GreaterThan"
        "threshold" : 10,
        "columnIndex" : 1
      }
    */
    val jsonConfigObj = JsonPath.parse(jsonConfig)
    val predicateType = jsonConfigObj.read[String]("$.predicateType")
    val threshold = jsonConfigObj.read[Int]("$.threshold")
    val columnIndex = jsonConfigObj.read[Int]("$.columnIndex")

    if (predicateType == "GreaterThan") {
      (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => {
        val indexFileName = segmentFileName.dropRight(3) + getIndexName()
        var shouldRead = true
        if (Files.exists(Paths.get(indexFileName))) {
          for (line <- Source.fromFile(indexFileName).getLines) {
            if (line.split(':')(0).toInt == columnIndex && line.split(':')(2).toInt <= threshold) {
              shouldRead = false
            }
          }
        }
        if (shouldRead) {
          Seq((KasparDriver.READ_WHOLE_SEGMENT, KasparDriver.READ_WHOLE_SEGMENT))
        } else {
          Seq((KasparDriver.DO_NOT_READ_SEGMENT, KasparDriver.DO_NOT_READ_SEGMENT))
        }
      }
    } else {
      // we don't understand this predicate so read everything
      (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => Seq((KasparDriver.READ_WHOLE_SEGMENT, KasparDriver.READ_WHOLE_SEGMENT))
    }
  }

  override def rowPredicateFromJson(jsonConfig: String): RawRow => Boolean = {
    /*
      json structure for greater than predicate:
      {
        "predicateType" : "GreaterThan"
        "threshold" : 10,
        "columnIndex" : 1
      }
    */
    val jsonConfigObj = JsonPath.parse(jsonConfig)
    val predicateType = jsonConfigObj.read[String]("$.predicateType")
    val threshold = jsonConfigObj.read[Int]("$.threshold")
    val columnIndex = jsonConfigObj.read[Int]("$.columnIndex")

    if (predicateType == "GreaterThan") {
      (row: RawRow) => {
        row.getIntVal(columnIndex) > threshold
      }
    } else {
      (row: RawRow) => true
    }
  }
}