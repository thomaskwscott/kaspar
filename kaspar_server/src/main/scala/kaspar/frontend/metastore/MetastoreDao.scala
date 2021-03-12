package kaspar.frontend.metastore

class MetastoreDao(val connectionPool: ConnectionPool
                    )
{
  val RESULT_STATEMENT = "SELECT * FROM %s_result"

  def getQueryResult(queryId: String): Option[String] = {
    connectionPool.getConnection().map(conn => {
      val statement = conn.createStatement()
      val result = statement.executeQuery(RESULT_STATEMENT.format(queryId))
      val columnCount = result.getMetaData.getColumnCount
      val results = new StringBuilder
      for( columnIndex <- 1 to columnCount) {
        results ++= result.getMetaData.getColumnName(columnIndex)
        results ++= (if (columnIndex == columnCount) "" else " | ")
      }
      results ++= "\n"
      while( result.next()) {
        for( columnIndex <- 1 to columnCount) {
          results++= result.getString(columnIndex)
          results++= (if (columnIndex == columnCount) "" else " | ")
        }
        results++= "\n"
      }
      conn.close()
      results.toString()
    })
  }


}
