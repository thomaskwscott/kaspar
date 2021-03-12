package kaspar.frontend.path

import akka.http.scaladsl.server

trait Path {

  def getName(): String

  def getPath(): server.Route

}
