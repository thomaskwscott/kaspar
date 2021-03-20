package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives.{_symbol2NR, complete, delete, get, parameters, path}
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.{ColumnResponse, ListResultsResponse, ResultResponse, ResultSetResponse, RowResponse, SubmitQueryResponse}

case class KasparQuery(id: String)

class ResultPath(val metastoreDao: MetastoreDao) extends Path {

  val name = "result"

  // macros for mapping json requests/responses
  implicit val columnResponseW = upickle.default.macroW[ColumnResponse]
  implicit val rowResponseW = upickle.default.macroW[RowResponse]
  implicit val resultSetResponseW = upickle.default.macroW[ResultSetResponse]
  implicit val resultResponseW = upickle.default.macroW[ResultResponse]
  implicit val listResultsResponseW = upickle.default.macroW[ListResultsResponse]

  override def getPath(): server.Route = {
    path(name) {
      Directives.concat(
        get {
          parameters('queryId.?) {
            queryId => {
              queryId match {
                case Some(queryId) => {
                  val results = metastoreDao.getQueryResult(queryId).getOrElse(ResultSetResponse(List.empty))
                  complete(
                    HttpEntity(ContentTypes.`application/json`,
                      upickle.default.write(results)
                    ))
                }
                case _ => {
                  val results = metastoreDao.getResults().getOrElse(ListResultsResponse(List.empty))
                  complete(
                    HttpEntity(ContentTypes.`application/json`,
                      upickle.default.write(results)
                    ))
                }
              }
            }
          }
        },
        delete {
          parameters("queryId") {
            queryId => {
              metastoreDao.deleteQueryResult(queryId)
              complete(
                HttpEntity(ContentTypes.`text/plain(UTF-8)`,
                  "Results Deleted"))
            }
          }
        })
    }
  }

  override def getName(): String = name
}