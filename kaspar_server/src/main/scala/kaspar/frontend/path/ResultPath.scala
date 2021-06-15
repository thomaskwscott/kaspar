package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives.{_symbol2NR, complete, delete, get, parameters, path}
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.{GetColumnResponse, ConversionUtils, GetResultResponseList, QueryResultSpecList, GetResultResponse, RowSpecList, GetRowResponseList, GetRowResponse, PostQueryResponse}

case class KasparQuery(id: String)

class ResultPath(val metastoreDao: MetastoreDao) extends Path {

  val name = "result"

  // macros for mapping json requests/responses
  implicit val columnResponseW = upickle.default.macroW[GetColumnResponse]
  implicit val rowResponseW = upickle.default.macroW[GetRowResponse]
  implicit val resultSetResponseW = upickle.default.macroW[GetRowResponseList]
  implicit val resultResponseW = upickle.default.macroW[GetResultResponse]
  implicit val listResultsResponseW = upickle.default.macroW[GetResultResponseList]

  override def getPath(): server.Route = {
    path(name) {
      Directives.concat(
        get {
          parameters('queryId.?) {
            queryId => {
              queryId match {
                case Some(queryId) => {
                  val results = metastoreDao.getQueryResult(queryId).getOrElse(RowSpecList(List.empty))
                  complete(
                    HttpEntity(ContentTypes.`application/json`,
                      upickle.default.write(ConversionUtils.convert(results))
                    ))
                }
                case _ => {
                  val results = metastoreDao.getResults().getOrElse(QueryResultSpecList(List.empty))
                  complete(
                    HttpEntity(ContentTypes.`application/json`,
                      upickle.default.write(ConversionUtils.convert(results))
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