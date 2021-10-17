package kaspar.frontend.metastore

import kaspar.frontend.model.{ColumnValueSpec, QueryResultSpec, QueryResultSpecList, QueryStatus, QueryStatusSpec, QueryStatusSpecList, RowSpec, RowSpecList, TableSpec, TableSpecList}
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
  val ALL_TABLES_STATEMENT = "SELECT table_id, table_name, table_spec FROM tables"
  val TABLE_ID_STATEMENT = "SELECT table_id, table_name, table_spec FROM tables WHERE table_id = %s"
  val TABLE_NAME_STATEMENT = "SELECT table_id, table_name, table_spec FROM tables WHERE table_name = '%s'"
  val DROP_RESULT_STATEMENT = "DROP TABLE %s_result"
  val LIST_RESULTS_STATEMENT= "SHOW TABLES LIKE '%_result'"
  val TABLES_TABLE_CREATE_STATEMENT = """
                                        |CREATE TABLE IF NOT EXISTS tables (
                                        |  table_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                        |  table_name VARCHAR(50),
                                        |  table_spec VARCHAR(5000)
                                        |)
    """.stripMargin
  val INSERT_TABLE_STATEMENT = """
                                  |INSERT INTO tables (
                                  |  table_name, table_spec
                                  |) VALUES (
                                  |'%s','%s'
                                  |)
                                  |""".stripMargin
  val DELETE_TABLE_STATEMENT = """
                                  |DELETE FROM tables
                                  |WHERE table_id = %s
                                  |""".stripMargin

  connectionPool.getConnection().map(conn => {
    conn.createStatement().executeUpdate(STATUS_TABLE_CREATE_STATEMENT)
    conn.createStatement().executeUpdate(TABLES_TABLE_CREATE_STATEMENT)
    conn.close()
  })

  def createTable(tableName: String, tableSpec: String): Option[Int] = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(
        INSERT_TABLE_STATEMENT.format(
          tableName,
          tableSpec
        ))
      val result = conn.createStatement().executeQuery(LAST_ID_STATEMENT)
      result.next()
      val columnId = result.getInt(1)
      conn.close()
      columnId
    })
  }

  def deleteTable(columnId: Int): Unit = {
    connectionPool.getConnection().map(conn => {
      conn.createStatement().executeUpdate(
        DELETE_TABLE_STATEMENT.format(
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

  def getTable(tableId: Int): Option[TableSpec] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(TABLE_ID_STATEMENT.format(tableId))
      result.next()
      val tableSpec = TableSpec(
        result.getInt(1),
        result.getString(2),
        result.getString(3)
      )
      conn.close()
      tableSpec
    })
  }

  def getTable(tableName: String): Option[TableSpec] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(TABLE_NAME_STATEMENT.format(tableName))
      result.next()
      val tableSpec = TableSpec(
        result.getInt(1),
        result.getString(2),
        result.getString(3)
      )
      conn.close()
      tableSpec
    })
  }

  def getAllTables(): Option[TableSpecList] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(ALL_TABLES_STATEMENT)
      val tableSpecs = new ListBuffer[TableSpec]()
      while( result.next()) {
        tableSpecs += TableSpec(
          result.getInt(1),
          result.getString(2),
          result.getString(3)
        )
      }
      conn.close()
      TableSpecList(tableSpecs.toList)
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
