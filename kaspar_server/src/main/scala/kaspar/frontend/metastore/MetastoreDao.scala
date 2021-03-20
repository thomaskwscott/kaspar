package kaspar.frontend.metastore

import kaspar.frontend.model.{ColumnResponse, ListResultsResponse, QueryStatus, QueryStatusResponse, QueryStatusResponses, ResultResponse, ResultSetResponse, RowResponse}
import kaspar.frontend.model.QueryStatus.QueryStatus

import scala.collection.mutable.ListBuffer

class MetastoreDao(val connectionPool: ConnectionPool
                    )
{

  val STATUS_TABLE_CREATE_STATEMENT = """
    |CREATE TABLE IF NOT EXISTS query_status (
    |  query_id VARCHAR(50) PRIMARY KEY,
    |  status VARCHAR(20)
    |)
    """.stripMargin
  val RESULT_STATEMENT = "SELECT * FROM %s_result"
  val STATUS_UPSERT_STATEMENT = """
    |INSERT INTO query_status (
    |  query_id, status
    |) VALUES (
    |'%s','%s'
    |) ON DUPLICATE KEY UPDATE
    |query_id = '%s', status = '%s'
    |""".stripMargin
  val ALL_STATUS_STATEMENT = "SELECT query_id, status FROM query_status"
  val STATUS_STATEMENT = "SELECT status FROM query_status WHERE query_id = '%s'"
  val DROP_RESULT_STATEMENT = "DROP TABLE %s_result"
  val LIST_RESULTS_STATEMENT= "SHOW TABLES LIKE '%_result'"

  connectionPool.getConnection().map(conn => {
    conn.createStatement().executeUpdate(STATUS_TABLE_CREATE_STATEMENT)
    conn.close()
  })

  def setQueryStatus(queryId: String, status: QueryStatus): Unit = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(
        STATUS_UPSERT_STATEMENT.format(
          queryId,
          status,
          queryId,
          status
        ))
      conn.close()
    })
  }

  def getQueryStatus(queryId: String): Option[QueryStatus] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(STATUS_STATEMENT.format(queryId))
      result.next()
      val status = QueryStatus.withName(result.getString(1))
      conn.close()
      status
    })
  }

  def getAllQueryStatuses(): Option[QueryStatusResponses] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(ALL_STATUS_STATEMENT)
      val statuses = new ListBuffer[QueryStatusResponse]()
      while( result.next()) {
        statuses += QueryStatusResponse(result.getString(1),result.getString(2))
      }
      conn.close()
      QueryStatusResponses(statuses.toList)
    })
  }

  def deleteQueryResult(queryId: String): Unit = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(DROP_RESULT_STATEMENT.format(queryId))
      conn.close()
    })
  }

  def getQueryResult(queryId: String): Option[ResultSetResponse] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(RESULT_STATEMENT.format(queryId))
      val columnCount = result.getMetaData.getColumnCount
      val columnNames = (1 to columnCount).map(i => result.getMetaData.getColumnName(i))
      val rowResponses = new ListBuffer[RowResponse]
      while( result.next()) {
        val colResponses = new ListBuffer[ColumnResponse]
        for( columnIndex <- 1 to columnCount) {
          val colValue = result.getString(columnIndex)
          colResponses += ColumnResponse(columnNames(columnIndex-1),colValue)
        }
        rowResponses += RowResponse(colResponses.toList)
      }
      conn.close()
      ResultSetResponse(rowResponses.toList)
    })
  }

  def getResults(): Option[ListResultsResponse] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(LIST_RESULTS_STATEMENT)
      val resultResponses = new ListBuffer[ResultResponse]
      while( result.next()) {
        resultResponses += ResultResponse(result.getString(1).dropRight(7))
      }
      conn.close()
      ListResultsResponse(resultResponses.toList)
    })
  }


}
