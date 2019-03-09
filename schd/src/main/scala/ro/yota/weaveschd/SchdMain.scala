package ro.yota.weaveschd

import akka.actor.ActorSystem
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import play.api.libs.json._
import scalax.file.Path
import org.rogach.scallop._
import scala.language.reflectiveCalls // required for scallop with subcommand
import java.util.concurrent.TimeoutException

import scala.language.postfixOps

trait RunnableSubcommand {
  def run(conf: SchdConf): Unit
}

class SchdConf(args: Seq[String]) extends ScallopConf(args) {
  val mainCommand = new Subcommand("main") with RunnableSubcommand {
    def run(conf: SchdConf): Unit = SchdMain.runMain(conf)
  }
  addSubcommand(mainCommand)
  val stopCommand = new Subcommand("stop") with RunnableSubcommand {
    def run(conf: SchdConf): Unit = SchdMain.runStop(conf)
  }
  addSubcommand(stopCommand)
  val statusCommand = new Subcommand("status") with RunnableSubcommand {
    val json = opt[Boolean]("json", 'j', "Output JSON format")
    def run(conf: SchdConf): Unit = SchdMain.runStatus(conf)
  }
  addSubcommand(statusCommand)
  val submitCommand = new Subcommand("submit") with RunnableSubcommand {
    def run(conf: SchdConf): Unit = SchdMain.runSubmit(conf)
  }
  addSubcommand(submitCommand)
  val checkCommand = new Subcommand("check") with RunnableSubcommand {
    def run(conf: SchdConf): Unit = SchdMain.runCheck(conf)
  }
  addSubcommand(checkCommand)

  verify()
}

/*
 Main class for builtin scheduler for weave
  Currently, this is also used as an entry point for internal call
  for API, we prefer to use Json for interfacing, and not to expose internal object
 */
object SchdMain  {

  val activeCheckPath = new java.io.File(System.getProperty("user.home") + "/.schd_active")

  def submit(jobjson: String) {
    submitMany("[" + jobjson + "]")
  }

  def submitMany(jobjson: String) {
    val sys = AkkaSchedulerKernel.clientActorSystem
    val sch = AkkaSchedulerKernel.remoteSchedulerActor(sys)

    val jobinfos = Json.parse(jobjson).as[Seq[JsObject]]
    val tasks = for (jobinfo <- jobinfos) yield {
      val jobid = (jobinfo \ "jobId").as[String]
      val stdout = (jobinfo \ "stdoutFile").asOpt[String].map({pstr =>
        Path.fromString(pstr).toAbsolute.path
      }).getOrElse("")
      val stderr = (jobinfo \ "stderrFile").asOpt[String].map({pstr =>
        Path.fromString(pstr).toAbsolute.path
      }).getOrElse("")
      val deps =
        (jobinfo \ "deps").asOpt[Seq[String]].getOrElse(List())
      //println(s"deps = ${deps}")
      val script = (jobinfo \ "script").as[String]
      val shell = (jobinfo \ "shell").asOpt[String].getOrElse("bash")

      val task = new Task(jobid, deps, script,
        stdout, stderr, shell)
      task
    }

    implicit val timeout = Timeout(30 seconds)
    Await.result(sch ? AddTasks(tasks), timeout.duration)
    sys.shutdown()
  }

  def queryStatusAll(): Seq[Task] = {
    val sys = AkkaSchedulerKernel.clientActorSystem
    try {
      val sch = AkkaSchedulerKernel.remoteSchedulerActor(sys)
      implicit val timeout = Timeout(30 seconds)

      var tasks: Option[Seq[Task]] = None
      Await.result(sch ? QueryTasks, timeout.duration) match {
        case QueryTasks_Ret(t) => {
          tasks = Some(t)
        }
      }
      if (tasks.isEmpty) {
        throw new RuntimeException("Connection failed");
      }
      tasks.get
    } finally {
      sys.shutdown()
    }
  }

  def statusAll(): JsValue = {
    if (this.checkAlive()) {
      Json.toJson(queryStatusAll())
    } else {
      Json.toJson(Seq(): Seq[JsObject])
    }
  }

  def checkAlive(): Boolean = {
    if (activeCheckPath.exists()) {
      try {
        val sys = AkkaSchedulerKernel.clientActorSystem
        val sch = AkkaSchedulerKernel.remoteSchedulerActor(sys)
        implicit val timeout = Timeout(1 seconds)
        Await.result(sch ? PingScheduler, timeout.duration) match {
          case RemoteAck => {
            true
          }
          case _ => {
            false // This should never happen
          }
        }
      } catch {
        case _: TimeoutException => false
      }
    } else {
      false
    }
  }

  def runMain(conf: SchdConf): Unit = {
    println(banner)
    println("Running AkkaScheduker " + ActorSystem.Version)

    val sch = new AkkaSchedulerKernel;
    sch.startup()

    println("Successfully started Akka")
    activeCheckPath.createNewFile()
    activeCheckPath.deleteOnExit()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      def run = {
        println("Shutting down AkkaScheduler...")
        sch.shutdown()
        println("Successfully shut down Akka")
      }
    }))
  }

  def runStop(conf: SchdConf): Unit = {
    val sys = AkkaSchedulerKernel.clientActorSystem
    val sch = AkkaSchedulerKernel.remoteSchedulerActor(sys)
    implicit val timeout = Timeout(30 seconds)
    Await.result(sch ? ShutdownScheduler, timeout.duration)
    sys.shutdown()
  }

  def runSubmit(conf: SchdConf): Unit = {
    val jobjson = scala.io.Source.stdin.mkString
    submit(jobjson)
  }

  def runStatus(conf: SchdConf): Unit = {
    val tasks = this.queryStatusAll()
    if (conf.statusCommand.json()) {
      println(Json.toJson(queryStatusAll()))
    } else {
      println("====================")
      for (task <- tasks) {
        println("")
        println(s"       task id: ${task.id}")
        println(s"        status: ${task.status.name}")
        if (task.status == TaskStatus_Pending) {
          println("  launch after: ")
          for (wait <- task.waitFor) {
            println("    " + wait)
          }
        }
        println("--------------------")
      }
    }
  }

  def runCheck(conf: SchdConf): Unit = {
    if (this.checkAlive()) {
      System.exit(0)
    } else {
      System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SchdConf(args)

    conf.subcommand match {
      case Some(cmd: RunnableSubcommand) => {
        cmd.run(conf)
      }
      case _ => {
      }
    }
  }


  private def banner = """
 _______ _______ _     _ ______
 |______ |       |_____| |     \
 ______| |_____  |     | |_____/

"""
}

