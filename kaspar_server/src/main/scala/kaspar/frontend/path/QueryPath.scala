package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{_symbol2NR, as, complete, entity, parameters, path, post}
import akka.http.scaladsl.server.{Directives, Route}
import kaspar.frontend.KasparRunner
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.{QueryStatus, QueryStatusResponse, QueryStatusResponses, SubmitQueryResponse}

import java.util.UUID

class QueryPath(
                 val kasparRunner: KasparRunner,
                 val metastoreUrl: String,
                 val metastoreDao: MetastoreDao
               ) extends Path {



  val name = "query"

  // macros for mapping json requests/responses
  implicit val submitQueryResponseW = upickle.default.macroW[SubmitQueryResponse]
  implicit val queryStatusResponseW = upickle.default.macroW[QueryStatusResponse]
  implicit val queryStatusResponsesW = upickle.default.macroW[QueryStatusResponses]

  override def getPath(): Route = {
    path(name) {
      Directives.concat(post {
        entity(as[String]) { statement =>
          val queryId = UUID.randomUUID().toString.replaceAll("-","_")
          kasparRunner.scheduleStatement(statement,queryId)
          complete(HttpEntity(ContentTypes.`application/json`,
            upickle.default.write(SubmitQueryResponse(queryId))
          ))
        }
      },
        Directives.get {
          parameters('queryId.?) {
            queryId => {
              queryId match {
                case Some(queryId) => {
                  val status = metastoreDao.getQueryStatus(queryId).getOrElse(QueryStatus.DOES_NOT_EXIST)
                  complete(HttpEntity(ContentTypes.`application/json`,
                    upickle.default.write(QueryStatusResponse(queryId,status.toString))
                  ))
                }
                case _ => {
                  val result = metastoreDao.getAllQueryStatuses().getOrElse(QueryStatusResponses(List.empty))
                  complete(HttpEntity(ContentTypes.`application/json`,
                    upickle.default.write(result)
                  ))
                }
              }

        }}})
    }
  }

  override def getName(): String = name
}
