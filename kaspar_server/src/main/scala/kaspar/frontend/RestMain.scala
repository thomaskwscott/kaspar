package kaspar.frontend

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import kaspar.frontend.metastore.{ConnectionPool, MetastoreDao}
import kaspar.frontend.path.{QueryPath, ResultPath, TablePath, VersionPath}

import java.io.FileInputStream
import java.util.Properties
import scala.io.StdIn

class RestMain(clientProperties: Properties) {

  implicit val system = ActorSystem(Behaviors.empty, "kaspar-system")
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.executionContext

  val metastoreDao = new MetastoreDao(ConnectionPool(clientProperties))
  val kasparRunner = new KasparRunner(clientProperties,metastoreDao)

  val metastoreUrl = clientProperties.getProperty(KasparServerConfig.METASTORE_JDBC_URL_CONFIG)
  val route =
    concat(
      new VersionPath().getPath(),
      new QueryPath(kasparRunner,metastoreUrl,metastoreDao).getPath(),
      new ResultPath(metastoreDao).getPath(),
      new TablePath(metastoreDao).getPath()
    )

  val port = clientProperties.getProperty("rest.port").toInt
  val bindingFuture = Http().newServerAt("localhost", port).bind(route)

  def start(): Unit = {
    println(s"Server online at http://localhost:" + port + "/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

}

object RestMain {

  val usage = """
    Usage: -c config file
  """

  def main(args: Array[String]): Unit = {
    // grab the properties
    if (args.length == 0) println(usage)
    def nextOption(map : Map[String, String], list: List[String]) : Map[String,String] = {
      list match {
        case Nil => map
        case "-c" :: value :: tail =>
          nextOption(map ++ Map("configFilePath" -> value.toString), tail)
        case option :: tail => println("Unknown option "+option)
          System.exit(1)
          map
      }
    }
    val options = nextOption(Map(),args.toList)
    val clientProps = new Properties()
    clientProps.load(new FileInputStream(options.get("configFilePath").get))
    val restMain = new RestMain(clientProps)
    restMain.start()
  }
}
