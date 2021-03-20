package kaspar.frontend

import kaspar.dataload.TopicLoader
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.structure.SimpleJsonValueColumnifier
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.QueryStatus
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import java.util.{Properties, UUID}
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import scala.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
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
