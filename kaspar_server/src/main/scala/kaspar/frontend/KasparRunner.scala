package kaspar.frontend

import kaspar.dataload.TopicLoader
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.structure.{PathJsonValueColumnifier, SimpleJsonValueColumnifier}
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.QueryStatus
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.util.TablesNamesFinder
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class KasparRunner (val clientProperties: Properties,
                    val metastoreDao: MetastoreDao) {

  val executorPool: ExecutorService = Executors.newFixedThreadPool(
    clientProperties.getProperty(KasparServerConfig.RUNNER_POOL_SIZE).toInt)

  val spark = SparkSession
    .builder()
    .appName("Kaspar Executable")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = new SQLContext(sc)

  def scheduleStatement(statement: String, queryId: String): Unit = {
    metastoreDao.setQueryStatus(queryId,QueryStatus.RUNNING)
    Future {
      val resultDf = runStatement(statement)
      resultDf.write.mode(SaveMode.Overwrite).jdbc(
        clientProperties.getProperty(KasparServerConfig.METASTORE_JDBC_URL_CONFIG),
        queryId + "_result",
        new Properties()
      )
    } (ExecutionContext.fromExecutor(executorPool)).onComplete {
      case Success(x) => {
        metastoreDao.setQueryStatus(queryId, QueryStatus.COMPLETE)
      }
      case Failure(e) => e.printStackTrace
    }(ExecutionContext.fromExecutor(executorPool))
  }

  def runStatement(statement: String): sql.DataFrame =  {
    val parsedStatement = parseStatement(statement)

    parsedStatement._1.foreach(tableDef => {
      val tableName = tableDef._1
      val tableCols = tableDef._2

      val tableColumnifier = new SimpleJsonValueColumnifier(tableCols)
      //val tableColumnifier = new PathJsonValueColumnifier(tableCols)

      val tableRawRows = TopicLoader.getRawRowsFromKafka(sc,tableName,clientProperties,tableColumnifier)
      TopicLoader.registerTableFromRdd(sqlContext,tableRawRows,tableName,tableCols)
    })

    sqlContext.sql(parsedStatement._2)
  }

  def parseStatement(statement: String): (Map[String,Array[(String,ColumnType.Value)]],String) = {

    val lines = statement.split("\n").groupBy(line => line.startsWith("# col: "))
    val sql = lines(false).mkString
    if(lines.contains(true)) {
      val explicitCols = lines(true).map(line => {
        val columnParts = line.drop(7).split(" ")
        val columnType = ColumnType.withName(columnParts(1))
        val tableName = columnParts(0).split("\\.")(0)
        val columnName = columnParts(0).split("\\.")(1)
        (tableName, columnName, columnType)
      }).groupBy(e => e._1).mapValues(cols => cols.map(nameType => (nameType._2, nameType._3)))
      (explicitCols, sql)
    } else {
      val cols = getImplicitColumns(statement);
      println(cols)
      (getImplicitColumns(statement), sql)
    }
  }

  def getImplicitColumns(statement: String): Map[String,Array[(String,ColumnType.Value)]] = {
    val parsedStatement = CCJSqlParserUtil.parse(statement)
    val selectStatement = parsedStatement.asInstanceOf[Select]
    val tablesNamesFinder = new TablesNamesFinder
    tablesNamesFinder.getTableList(selectStatement).asScala.flatMap(tableName => {
      metastoreDao.getTableColumns(tableName).get.columnSpecs.map(
        spec => (spec.tableName, spec.columnName,ColumnType.withName(spec.columnType.toString)))
    }).groupBy(e => e._1).mapValues(cols => cols.map(nameType => (nameType._2,nameType._3)).toArray)
  }
}
