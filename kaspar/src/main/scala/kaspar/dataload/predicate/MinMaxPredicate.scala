package kaspar.dataload.predicate

import java.io.File
import java.nio.file.{Paths, Files}

import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.metadata.ColumnType.ColumnType
import kaspar.dataload.structure.{Columnifier, RawRow}
import org.apache.spark.sql.types.DataTypes

import scala.io.Source

object MinMaxPredicate {

  def getIndexName() : String = {
    "minMax.index"
  }

  def getIndexFunction(columnsToIndex: Seq[(Int,ColumnType)]) : Seq[RawRow] => String = {
    records: Seq[RawRow] => {
      columnsToIndex.map(column => {
        val columnRecords = records.map(rawRow => {
          column._2 match {
            case ColumnType.LONG => rawRow.getLongVal(column._1)
            case ColumnType.DOUBLE => rawRow.getDoubleVal(column._1)
            case ColumnType.INTEGER => rawRow.getIntVal(column._1)
            case _ => rawRow.getStringVal(column._1)
          }
        })
        if(!columnRecords.isEmpty) {
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

  def buildGreaterThanSegmentPredicate(threshold: Int, columnIndex: Int) : (Seq[File],String, Int, String) => Boolean =  {
    (partitionFiles: Seq[File], topicName: String, partition: Int, segmentFileName: String) => {
      val indexFileName = segmentFileName.dropRight(3) + getIndexName()
      var shouldRead = true
      if( Files.exists(Paths.get(indexFileName))) {
        for (line <- Source.fromFile(indexFileName).getLines) {
          if (line.split(':')(0).toInt == columnIndex && line.split(':')(2).toInt <= threshold) {
            shouldRead = false
          }
        }
      }
      shouldRead
    }
  }

}
