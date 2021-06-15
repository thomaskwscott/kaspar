package kaspar.frontend.metastore

import kaspar.frontend.model.ColumnType.ColumnType
import kaspar.frontend.model.{ColumnSpec, ColumnSpecList, ColumnType, ColumnValueSpec, GetColumnResponse, GetQueryStatusResponse, GetQueryStatusResponseList, GetResultResponse, GetResultResponseList, GetRowResponse, GetRowResponseList, QueryResultSpec, QueryResultSpecList, QueryStatus, QueryStatusSpec, QueryStatusSpecList, RowSpec, RowSpecList}
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
  val LAST_ID_STATEMENT = "SELECT LAST_INSERT_ID()"
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
  val ALL_COLUMNS_STATEMENT = "SELECT column_id, table_name, column_name, column_type FROM columns"
  val COLUMN_STATEMENT = "SELECT column_id, table_name, column_name, column_type FROM columns WHERE column_id = %s"
  val DROP_RESULT_STATEMENT = "DROP TABLE %s_result"
  val LIST_RESULTS_STATEMENT= "SHOW TABLES LIKE '%_result'"
  val COLUMN_TABLE_CREATE_STATEMENT = """
                                        |CREATE TABLE IF NOT EXISTS columns (
                                        |  column_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                        |  table_name VARCHAR(50),
                                        |  column_name VARCHAR(50),
                                        |  column_type VARCHAR(50)
                                        |)
    """.stripMargin
  val INSERT_COLUMN_STATEMENT = """
                                  |INSERT INTO columns (
                                  |  table_name, column_name, column_type
                                  |) VALUES (
                                  |'%s','%s','%s'
                                  |)
                                  |""".stripMargin
  val DELETE_COLUMN_STATEMENT = """
                                  |DELETE FROM columns
                                  |WHERE column_id = %s
                                  |""".stripMargin
  val TABLE_COLUMNS_STATEMENT = "SELECT column_id, table_name, column_name, column_type FROM columns WHERE table_name = '%s'"

  connectionPool.getConnection().map(conn => {
    conn.createStatement().executeUpdate(STATUS_TABLE_CREATE_STATEMENT)
    conn.createStatement().executeUpdate(COLUMN_TABLE_CREATE_STATEMENT)
    conn.close()
  })

  def createColumn(tableName: String, columnName: String, columnType: ColumnType): Option[Int] = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(
        INSERT_COLUMN_STATEMENT.format(
          tableName,
          columnName,
          columnType
        ))
      val result = conn.createStatement().executeQuery(LAST_ID_STATEMENT)
      result.next()
      val columnId = result.getInt(1)
      conn.close()
      columnId
    })
  }

  def deleteColumn(columnId: Int): Unit = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(
        DELETE_COLUMN_STATEMENT.format(
          columnId
        ))
      conn.close()
    })
  }

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

  def getColumn(columnId: Int): Option[ColumnSpec] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(COLUMN_STATEMENT.format(columnId))
      result.next()
      val columnSpec = ColumnSpec(
        result.getInt(1),
        result.getString(3),
        result.getString(2),
        ColumnType.withName(result.getString(4))
      )
      conn.close()
      columnSpec
    })
  }

  def getTableColumns(tableName: String): Option[ColumnSpecList] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(TABLE_COLUMNS_STATEMENT.format(tableName))
      val columnSpecs = new ListBuffer[ColumnSpec]()
      while( result.next()) {
        columnSpecs += ColumnSpec(
          result.getInt(1),
          result.getString(3),
          result.getString(2),
          ColumnType.withName(result.getString(4))
        )
      }
      conn.close()
      ColumnSpecList(columnSpecs.toList)
    })
  }

  def getAllColumns(): Option[ColumnSpecList] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(ALL_COLUMNS_STATEMENT)
      val columnSpecs = new ListBuffer[ColumnSpec]()
      while( result.next()) {
        columnSpecs += ColumnSpec(
          result.getInt(1),
          result.getString(2),
          result.getString(3),
          ColumnType.withName(result.getString(4))
        )
      }
      conn.close()
      ColumnSpecList(columnSpecs.toList)
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

  def getAllQueryStatuses(): Option[QueryStatusSpecList] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(ALL_STATUS_STATEMENT)
      val statuses = new ListBuffer[QueryStatusSpec]()
      while( result.next()) {
        statuses += QueryStatusSpec(result.getString(1),result.getString(2))
      }
      conn.close()
      QueryStatusSpecList(statuses.toList)
    })
  }

  def deleteQueryResult(queryId: String): Unit = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(DROP_RESULT_STATEMENT.format(queryId))
      conn.close()
    })
  }

  def getQueryResult(queryId: String): Option[RowSpecList] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(RESULT_STATEMENT.format(queryId))
      val columnCount = result.getMetaData.getColumnCount
      val columnNames = (1 to columnCount).map(i => result.getMetaData.getColumnName(i))
      val rowSpecs = new ListBuffer[RowSpec]
      while( result.next()) {
        val colSpecs = new ListBuffer[ColumnValueSpec]
        for( columnIndex <- 1 to columnCount) {
          val colValue = result.getString(columnIndex)
          colSpecs += ColumnValueSpec(columnNames(columnIndex-1),colValue)
        }
        rowSpecs += RowSpec(colSpecs.toList)
      }
      conn.close()
      RowSpecList(rowSpecs.toList)
    })
  }

  def getResults(): Option[QueryResultSpecList] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(LIST_RESULTS_STATEMENT)
      val resultSpecs = new ListBuffer[QueryResultSpec]
      while( result.next()) {
        resultSpecs += QueryResultSpec(result.getString(1).dropRight(7))
      }
      conn.close()
      QueryResultSpecList(resultSpecs.toList)
    })
  }

}
