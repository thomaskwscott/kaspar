package integration

import scala.sys.process._
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import com.google.common.base.Throwables
import com.jayway.jsonpath.JsonPath

import java.io.{File, FileInputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Properties
import scala.io.Source

object TestRunner {

  private val logger = org.log4s.getLogger
  private val REMOTE_LOG_DIR = "/home/ubuntu/kaspar/log/"
  private val LOCAL_LOG_DIR = "src/test/resources/log/"
  private val LOG_FILE = "results.txt"

  def run(testClass: String): Unit = {

    // clean up previous run
    deleteResults()

    val container = DockerComposeContainer(
      new File("docker-compose.yml"),
      exposedServices = (Seq(ExposedService("master_1", 7077))))

    container.start()
    try {

      // wait for worker to be up
      val workerUrl = new URL("http://localhost:8083");
      var workerIsUp = false;
      while (!workerIsUp) {
        try {
          Thread.sleep(10000)
          val huc = workerUrl.openConnection().asInstanceOf[HttpURLConnection]
          workerIsUp = huc.getResponseCode() == 200
        } catch {
          case _ => logger.info("worker is not yet up")
        }
      }

      // submit
      val artifactProps = new Properties()
      artifactProps.load(new FileInputStream("target/classes/artifact.properties"))
      val kasparVersion = artifactProps.get("kaspar.version")

      val masterContainer = container.getContainerByServiceName("master_1").get
      val setupResult = masterContainer.execInContainer("sh", "-c", "/home/ubuntu/integration_test_setup.sh")
      val setupLogMessage = "Setup exited with: " + setupResult.getExitCode + "\n" +
        "stdErr:" + setupResult.getStderr + "\n" +
        "stdOut:" + setupResult.getStdout
      if(setupResult.getExitCode == 0) {
        logger.info(setupLogMessage)
      } else {
        logger.error(setupLogMessage)
      }
      val result = masterContainer.execInContainer("/usr/local/spark/bin/spark-submit",
        "--class", testClass,
        "--deploy-mode",  "cluster",
        "--supervise",
        "--master", "spark://master:7077",
        "--num-executors", "2",
        "--conf", "spark.blacklist.enabled=true",
        "--conf", "spark.blacklist.killBlacklistedExecutors=false",
        "--conf", "spark.blacklist.timeout=1s",
        "--conf", "spark.blacklist.task.maxTaskAttemptsPerNode=1",
        "--conf", "spark.scheduler.blacklist.unschedulableTaskSetTimeout=1s",
        "--jars", "/home/ubuntu/kaspar/target/kaspar-" + kasparVersion + "-fat-tests.jar",
        "/home/ubuntu/kaspar/target/kaspar-" + kasparVersion + "-fat-tests.jar")
      if(result.getExitCode!=0) {
        println("spark-submit failed with exit code:" + result.getExitCode)
        println("stderr:" + result.getStderr)
        println("stdout:" + result.getStdout)
        throw new RuntimeException("Test submission failed")
      }

      // wait for  job completion and report status
      var appRunning = true
      while (appRunning) {
        Thread.sleep(5000)
        val rawSpark = scala.io.Source.fromURL("http://localhost:8080/json/").mkString
        val sparkStatus = JsonPath.parse(rawSpark)
        val completedCount = sparkStatus.read[Int]("$.completeddrivers.length()")
        val activeCount = sparkStatus.read[Int]("$.activedrivers.length()")
        logger.info("Found " + activeCount + " active drivers and " + completedCount + " completed.")
        if (completedCount > 0 && activeCount == 0 ) {
          val completedState = sparkStatus.read[String]("$.completeddrivers[0].state")
          logger.info("Driver completed with state: " + completedState)
          val appResults = getResults()
          logger.info("Result: \n" + appResults)
          if( completedState != "FINISHED") {
            throw new RuntimeException("Test failed: " + appResults)
          }
          appRunning = false
        }
      }
    } finally {
      container.stop()
    }
  }

  def fail(): Unit = {
    val rawSpark = scala.io.Source.fromURL("http://master:8080/json/").mkString
    val sparkStatus = JsonPath.parse(rawSpark)
    val driverId = sparkStatus.read[String]("$.activedrivers[0].id")
    logger.info("Killing: " + driverId)
    Seq("curl",
    "-X", "POST",
    "http://master:8080/driver/kill/?id=" + driverId + "&terminate=true") ! ProcessLogger(stdout append _, stderr append _)
  }

  def fail(e: Throwable): Unit = {
    Files.write(Paths.get(REMOTE_LOG_DIR + LOG_FILE),
      Throwables.getStackTraceAsString(e).getBytes(StandardCharsets.UTF_8))
    fail()
    System.exit(1)
  }

  def recordResult(result: String): Unit = {
    Files.write(Paths.get(REMOTE_LOG_DIR + LOG_FILE), result.getBytes(StandardCharsets.UTF_8))
  }

  private def deleteResults(): Unit = {
    val resultsFile = new File(LOCAL_LOG_DIR +  LOG_FILE)
    if (resultsFile.exists) {
      resultsFile.delete()
    }
  }

  private def getResults(): String = {
    try {
      Source.fromFile(LOCAL_LOG_DIR + LOG_FILE).mkString
    } catch {
      case e => {
        logger.error(e.toString)
        ""
      }
    }
  }
}

object DOCKER extends org.scalatest.Tag("DOCKER") {
}
