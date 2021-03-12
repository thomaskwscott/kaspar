package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{as, complete, entity, path, post}
import akka.http.scaladsl.server.Route
import kaspar.frontend.KasparRunner
import org.apache.spark.sql.SaveMode

import java.util.{Properties, UUID}

class QueryPath(
                 val kasparRunner: KasparRunner,
                 val metastoreUrl: String
               ) extends Path {



  val name = "query"

  override def getPath(): Route = {
    path(name) {
      post {
        entity(as[String]) { statement =>
          val resultDf = kasparRunner.runStatement(statement)
          val queryId = UUID.randomUUID().toString.replaceAll("-","_")
          resultDf.write.mode(SaveMode.Overwrite).jdbc(
            metastoreUrl,
            queryId + "_result",
            new Properties()
          )
          complete(HttpEntity(ContentTypes.`application/json`,
            s"""
               |{ "query_id": "$queryId" }
               |""".stripMargin.replaceAll("\n","")))
        }
      }
    }
  }

  override def getName(): String = name
}
