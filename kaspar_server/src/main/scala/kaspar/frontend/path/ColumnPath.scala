package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{_symbol2NR, as, complete, entity, parameters, path}
import akka.http.scaladsl.server.{Directives, Route}
import kaspar.frontend.metastore.MetastoreDao
import kaspar.frontend.model.{ColumnSpec, ColumnType, ColumnSpecList, GetColumnSpecsResponseList, ConversionUtils, PostColumnSpecRequest, PostColumnSpecResponse, GetColumnSpecResponse}


class ColumnPath(
                  val metastoreDao: MetastoreDao
                ) extends Path {



  val name = "column"

  // macros for mapping json requests/responses
  implicit val createColumnResponseW = upickle.default.macroW[PostColumnSpecResponse]
  implicit val getColumnResponseW = upickle.default.macroW[GetColumnSpecResponse]
  implicit val createColumnRequestR = upickle.default.macroR[PostColumnSpecRequest]
  implicit val columnsResponseW = upickle.default.macroW[GetColumnSpecsResponseList]

  override def getPath(): Route = {
    path(name) {
      Directives.concat(
        Directives.post {
          entity(as[String]) { columnSpec =>
            val createRequest = upickle.default.read[PostColumnSpecRequest](columnSpec)
            val columnId = metastoreDao.createColumn(
              createRequest.tableName,
              createRequest.columnName,
              ColumnType.withName(createRequest.columnType)
            )
            complete(HttpEntity(ContentTypes.`application/json`,
              upickle.default.write(PostColumnSpecResponse(
                columnId.getOrElse(-1)
              ))
            ))
          }
        },
        Directives.get {
          parameters('columnId.?,'tableName.?) {
            (columnId, tableName) => {
              columnId match {
                case Some(columnId) => {
                  val columnSpec = metastoreDao.getColumn(columnId.toInt).getOrElse(ColumnSpec(-1,"","",ColumnType.INTEGER))
                  complete(HttpEntity(ContentTypes.`application/json`,
                    upickle.default.write(ConversionUtils.convert(columnSpec))
                  ))
                }
                case _ => {
                  tableName match {
                    case Some(tableName) => {
                      val result = metastoreDao.getTableColumns(tableName).getOrElse(ColumnSpecList(List.empty))
                      complete(HttpEntity(ContentTypes.`application/json`,
                        upickle.default.write(ConversionUtils.convert(result))
                      ))
                    }
                    case _ => {
                      val result = metastoreDao.getAllColumns().getOrElse(ColumnSpecList(List.empty))
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
          parameters('columnId.?) {
            columnId => {
              columnId match {
                case Some(columnId) => {
                  metastoreDao.deleteColumn(columnId.toInt)
                  complete(HttpEntity(ContentTypes.`application/json`,s"""
                                                                         |{ "info": "deleted column with id: $columnId }
                                                                         |""".stripMargin)
                  )
                }
                case _ => {
                  complete(HttpEntity(ContentTypes.`application/json`,s"""
                                                                         |{ "error": "no columnId provided" }
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
