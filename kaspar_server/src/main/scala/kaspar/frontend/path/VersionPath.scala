package kaspar.frontend.path

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{complete, get, path}

class VersionPath extends Path {

  val name = "version"

  override def getPath(): server.Route = {
    path(name) {
      get {
        complete(HttpEntity(ContentTypes.`application/json`, s"""
                                                                |{ "version": "0.9" }
                                                                |""".stripMargin))
      }
    }
  }

  override def getName(): String = name
}
