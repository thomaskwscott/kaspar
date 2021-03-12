package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{complete, get, parameters, path}
import kaspar.frontend.metastore.MetastoreDao

case class KasparQuery(id:String)

class ResultPath(val metastoreDao: MetastoreDao) extends Path {

  val name = "result"

  override def getPath(): server.Route = {
    path(name) {
      get {
        parameters("query_id") {
          queryId => {
            val results = metastoreDao.getQueryResult(queryId).getOrElse("")
            complete(
              HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                results))
          }
        }
      }
    }
  }

  override def getName(): String = name
}