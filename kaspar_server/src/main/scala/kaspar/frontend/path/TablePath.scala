package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{_symbol2NR, as, complete, entity, parameters, path}
import akka.http.scaladsl.server.{Directives, Route}
import com.fasterxml.jackson.databind.ObjectMapper
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model._


class TablePath(
                  val metastoreDao: MetastoreDao
                ) extends Path {



  val name = "table"

  // macros for mapping json requests/responses
  implicit val createTableResponseW = upickle.default.macroW[PostTableSpecResponse]
  implicit val getTableResponseW = upickle.default.macroW[GetTableSpecResponse]
  implicit val createTableRequestR = upickle.default.macroR[PostTableSpecRequest]
  implicit val tablesResponseW = upickle.default.macroW[GetTableSpecsResponseList]

  override def getPath(): Route = {
    path(name) {
      Directives.concat(
        Directives.post {
          entity(as[String]) { tableSpecRequest =>
            // can't use uPickle here as we want a string representation of a json node
            // for tableSpec
            val objectMapper = new ObjectMapper()
            val tableSpecParsed = objectMapper.readTree(tableSpecRequest)
            val tableName = tableSpecParsed.get("tableName").asText()
            val tableSpec = objectMapper.writeValueAsString(tableSpecParsed.get("tableSpec"))

            val createRequest = PostTableSpecRequest(tableName, tableSpec)
            val tableId = metastoreDao.createTable(
              createRequest.tableName,
              createRequest.tableSpec
            )
            complete(HttpEntity(ContentTypes.`application/json`,
              upickle.default.write(PostTableSpecResponse(
                tableId.getOrElse(-1)
              ))
            ))
          }
        },
        Directives.get {
          parameters('tableId.?,'tableName.?) {
            (tableId, tableName) => {
              tableId match {
                case Some(tableId) => {
                  val tableSpec = metastoreDao.getTable(tableId.toInt).getOrElse(TableSpec(-1,"",""))
                  complete(HttpEntity(ContentTypes.`application/json`,
                    upickle.default.write(ConversionUtils.convert(tableSpec))
                  ))
                }
                case _ => {
                  tableName match {
                    case Some(tableName) => {
                      val tableSpec = metastoreDao.getTable(tableName).getOrElse(TableSpec(-1,"",""))
                      complete(HttpEntity(ContentTypes.`application/json`,
                        upickle.default.write(ConversionUtils.convert(tableSpec))
                      ))
                    }
                    case _ => {
                      val result = metastoreDao.getAllTables().getOrElse(TableSpecList(List.empty))
                      complete(HttpEntity(ContentTypes.`application/json`,
                        upickle.default.write(ConversionUtils.convert(result))
                      ))
                    }
                  }

                }
              }
            }
          }
        },
        Directives.delete {
          parameters('tableId.?) {
            tableId => {
              tableId match {
                case Some(tableId) => {
                  metastoreDao.deleteTable(tableId.toInt)
                  complete(HttpEntity(ContentTypes.`application/json`,s"""
                                                                         |{ "info": "deleted table with id: $tableId }
                                                                         |""".stripMargin)
                  )
                }
                case _ => {
                  complete(HttpEntity(ContentTypes.`application/json`,s"""
                                                                         |{ "error": "no tableId provided" }
                                                                         |""".stripMargin)
                  )
                }
              }
            }
          }
        }
      )
    }
  }

  override def getName(): String = name
}
