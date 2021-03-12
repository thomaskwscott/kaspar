package kaspar.frontend

import kaspar.dataload.TopicLoader
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.structure.SimpleJsonValueColumnifier
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import java.util.Properties
import scala.io.Source

class KasparRunner (val clientProperties: Properties) {

  val spark = SparkSession
    .builder()
    .appName("Kaspar Executable")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = new SQLContext(sc)

  def runStatement(statement: _root_.scala.Predef.String): sql.DataFrame =  {
    val parsedStatement = parseStatement(statement)

    parsedStatement._1.foreach(tableDef => {
      val tableName = tableDef._1
      val tableCols = tableDef._2

      val tableColumnifier = new SimpleJsonValueColumnifier(tableCols)

      val tableRawRows = TopicLoader.getRawRowsFromKafka(sc,tableName,clientProperties,tableColumnifier)
      TopicLoader.registerTableFromRdd(sqlContext,tableRawRows,tableName,tableCols)
    })

    sqlContext.sql(parsedStatement._2)
  }

  def parseStatement(statement: String): (Map[String,Array[(String,ColumnType.Value)]],String) = {

    val lines = statement.split("\n").groupBy(line => line.startsWith("# col: "))
    val cols = lines(true).map(line => {
      val columnParts = line.drop(7).split(" ")
      val columnType = ColumnType.withName(columnParts(1))
      val tableName = columnParts(0).split("\\.")(0)
      val columnName = columnParts(0).split("\\.")(1)
      (tableName,columnName,columnType)
    }).groupBy(e => e._1).mapValues(cols => cols.map(nameType => (nameType._2,nameType._3)))
    val sql = lines(false).mkString
    (cols, sql)
  }
}
