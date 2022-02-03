package kaspar.frontend

import com.fasterxml.jackson.databind.ObjectMapper
import com.jayway.jsonpath.JsonPath
import kaspar.dataload.KasparDriverBuilder
import kaspar.dataload.metadata.ColumnType
import kaspar.dataload.predicate.PredicateGenerator
import kaspar.dataload.structure.{RawRow, RowDeserializer}
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.QueryStatus
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.util.TablesNamesFinder
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import java.io.File
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
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

    parsedStatement._1.foreach(tableName => {

    /*
      Example tableSpec:
      {
        "deserializerClass": "kaspar.dataload.structure.PathJsonValueRowDeserializer",
        "config": {
          "columns" : [
            {
              "name": "someCol",
              "type": "LONG",
              "path": "$.someCol"
            }
          ]
        },
        "predicates": [
          {
            "generatorClass": "kaspar.dataload.predicate.OffsetPredicateGenerator",
            "type": "SEGMENT",
            "config": {
              "predicateType": "GreaterThan"
              "partitionThresholds" : [
                { "partition": 0, "threshold": 10 },
                { "partition": 1, "threshold": 20 }
              ]
            }
          }
        ]
      }
    */

      val objectMapper = new ObjectMapper()
      val tableSpec = objectMapper.readTree(metastoreDao.getTable(tableName).get.tableSpec)

      val topicName = if (tableSpec.has("topicName") && !tableSpec.get("topicName").asText().isEmpty)
        tableSpec.get("topicName").asText() else tableName

      val tableDeserializerClass = Class.forName(tableSpec.get("deserializerClass").asText())
      val tableDeserializer = tableDeserializerClass.newInstance().asInstanceOf[RowDeserializer]
      val tableConfig  = tableSpec.get("config");
      val deserializerConfig = objectMapper.writeValueAsString(tableConfig)
      tableDeserializer.configure(deserializerConfig)

      val columnsArrayNode = tableConfig.get("columns")
      val tableCols = columnsArrayNode.asScala.map( columnNode => {
        val columnName = columnNode.get("name").asText()
        val columnType = ColumnType.withName(columnNode.get("type").asText())
        (columnName, columnType)
      }).toSeq

      val predicatesArrayNode = tableSpec.get("predicates")
      val rowPredicates = new mutable.ListBuffer[(RawRow => Boolean)]
      val segmentPredicates = new mutable.ListBuffer[((Seq[File], String, Int, String) => Boolean)]

      predicatesArrayNode.forEach( predicateNode => {
        val generatorClass = Class.forName(predicateNode.get("generatorClass").asText())
        val predicateType = predicateNode.get("type").asText()
        val predicateConfig = objectMapper.writeValueAsString(predicateNode.get("config"))
        val generator = generatorClass.newInstance().asInstanceOf[PredicateGenerator]
        if (predicateType == "SEGMENT") {
          segmentPredicates += generator.segmentPredicateFromJson(predicateConfig)
        } else {
          rowPredicates += generator.rowPredicateFromJson(predicateConfig)
        }
      })

      val kasparDriver = KasparDriverBuilder()
        .withClientProperties(clientProperties)
        .build()

      val tableRawRows = kasparDriver.getRows(
        sc,
        topicName,
        tableDeserializer,
        rowPredicates.toArray,
        segmentPredicates.toArray
      )
      kasparDriver.registerTable(sqlContext,tableRawRows,tableName,tableCols)
    })

    sqlContext.sql(parsedStatement._2)
  }

  def parseStatement(statement: String): (Seq[String],String) = {
    val tables = getTables(statement);
    (tables, statement)
  }

  def getTables(statement: String): Seq[String] = {
    val parsedStatement = CCJSqlParserUtil.parse(statement)
    val selectStatement = parsedStatement.asInstanceOf[Select]
    val tablesNamesFinder = new TablesNamesFinder
    tablesNamesFinder.getTableList(selectStatement).asScala
  }
}
