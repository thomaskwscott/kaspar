package kaspar.frontend.metastore

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import kaspar.frontend.KasparExecutableConfig
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.Properties

abstract class ConnectionPool{

  def getConnection(): Option[Connection]
}

object ConnectionPool {

  def apply(clientProperties: Properties): ConnectionPool = {
    val className = clientProperties.getProperty(KasparExecutableConfig.METASTORE_JDBC_CLASS_CONFIG)
    val jdbcUrl = clientProperties.getProperty(KasparExecutableConfig.METASTORE_JDBC_URL_CONFIG)
    val poolMinSize = clientProperties.getProperty(KasparExecutableConfig.METASTORE_JDBC_POOL_MIN_SIZE_CONFIG).toInt
    val poolMaxSize = clientProperties.getProperty(KasparExecutableConfig.METASTORE_JDBC_POOL_MAX_SIZE_CONFIG).toInt
    new SimpleConnectionPool(className,jdbcUrl,poolMinSize,poolMaxSize)
  }

  private class SimpleConnectionPool(className: String,
                               jdbcUrl: String,
                               minConnections: Int,
                               maxConnections: Int
                              ) extends ConnectionPool {
    val logger = LoggerFactory.getLogger(this.getClass)

    private val connectionPool = {
      try {
        Class.forName(className)
        val config = new BoneCPConfig()
        config.setJdbcUrl(jdbcUrl)
        config.setMinConnectionsPerPartition(minConnections)
        config.setMaxConnectionsPerPartition(maxConnections)
        config.setPartitionCount(1)
        config.setCloseConnectionWatch(true)// if connection is not closed throw exception
        config.setLogStatementsEnabled(true) // for debugging purpose
        Some(new BoneCP(config))
      } catch {
        case exception: Exception =>;
          logger.warn("Error in creation of connection pool"+exception.printStackTrace())
          None
      }
    }

    def getConnection: Option[Connection] = {
      connectionPool match {
        case Some(connPool) => Some(connPool.getConnection)
        case None => None
      }
    }
  }
}

