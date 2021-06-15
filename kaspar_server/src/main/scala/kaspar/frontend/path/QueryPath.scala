package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{_symbol2NR, as, complete, entity, parameters, path, post}
import akka.http.scaladsl.server.{Directives, Route}
import kaspar.frontend.KasparRunner
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.{ConversionUtils, QueryStatus, GetQueryStatusResponse, GetQueryStatusResponseList, QueryStatusSpecList, PostQueryResponse}

import java.util.UUID

class QueryPath(
                 val kasparRunner: KasparRunner,
                 val metastoreUrl: String,
                 val metastoreDao: MetastoreDao
               ) extends Path {



  val name = "query"

  // macros for mapping json requests/responses
  implicit val submitQueryResponseW = upickle.default.macroW[PostQueryResponse]
  implicit val queryStatusResponseW = upickle.default.macroW[GetQueryStatusResponse]
  implicit val queryStatusResponsesW = upickle.default.macroW[GetQueryStatusResponseList]

  override def getPath(): Route = {
    path(name) {
      Directives.concat(post {
        entity(as[String]) { statement =>
          val queryId = UUID.randomUUID().toString.replaceAll("-","_")
          kasparRunner.scheduleStatement(statement,queryId)
          complete(HttpEntity(ContentTypes.`application/json`,
            upickle.default.write(PostQueryResponse(queryId))
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
                    upickle.default.write(GetQueryStatusResponse(queryId,status.toString))
                  ))
                }
                case _ => {
                  val result = metastoreDao.getAllQueryStatuses().getOrElse(QueryStatusSpecList(List.empty))
                  complete(HttpEntity(ContentTypes.`application/json`,
                    upickle.default.write(ConversionUtils.convert(result))
                  ))
                }
              }

        }}})
    }
  }

  override def getName(): String = name
}
