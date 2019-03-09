package ro.yota.weave


import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
//import akka.event.Logging
import scalax.io.Resource
import scala.sys.process.ProcessLogger
import scala.sys.process._
import scalax.file.Path
import scalax.file.ImplicitConversions._
import scala.util.matching.Regex
import ro.yota.weave.storage._
import play.api.libs.json._
import play.api.libs.json.Json._
import com.typesafe.scalalogging.Logger

import org.rogach.scallop._
import scala.language.reflectiveCalls // required for scallop with subcommand

/**
  * Class for command-line arguments defined with scallop
  */
class WeaveConf(args: Seq[String]) extends ScallopConf(args) {
  val config = opt[String]("config", 'c', descr = "Config file", required = true)
  val jars = opt[List[String]]("jar", 'j', descr = "JAR file")
  val verbose = opt[Boolean]("verbose", 'v', descr = "verbose")
  val debug = opt[Boolean]("debug", descr = "debug")

  val orderCommand = new Subcommand("order") {
    val redo = opt[Boolean]("redo", descr = "Redo", noshort = true)
    val dryRun = opt[Boolean]("dryrun", descr = "Do dry run", noshort = true)
    val ignoreCheckPoint = opt[Boolean]("dont-use-checkpoint",
      descr = "Do not use checkpoint (but overwrite them)", noshort = true)
    val relaxCheckPoint = opt[Boolean]("allow-inconsistent-checkpoint",
      descr = "Allows inconsistent checkpoint", noshort = true)
    val objects = trailArg[List[String]](name = "List of objects")
  }
  addSubcommand(orderCommand)

  val execCommand = new Subcommand("exec") {
    val launchInfo = trailArg[String](name = "Launch info file")
  }
  addSubcommand(execCommand)

  val statusCommand = new Subcommand("status") {
    val orderId = trailArg[String](name = "OrderId", required = false)
  }
  addSubcommand(statusCommand)

  val storeLogCommand = new Subcommand("store-log") {
    val launchInfo = trailArg[String](name = "Launch info file")
    val outLog = trailArg[String](name = "stdout file")
    val errLog = trailArg[String](name = "stderr file")
  }
  addSubcommand(storeLogCommand)

  val showCommand = new Subcommand("show") {
    val waitOrder = opt[Boolean]("wait", 'w', descr = "wait")
    val objectName = trailArg[String](name = "Object Name")
  }
  addSubcommand(showCommand)

  val showLogCommand = new Subcommand("showlog", "show-log") {
    val objectNames = trailArg[List[String]](name = "Object Names")
  }
  addSubcommand(showLogCommand)

  val listOrderCommand = new Subcommand("list-orders", "list-order") {
    val number = opt[Int]("num-orders", 'n', descr = "Number of orders to be listed", default = Some(10))
  }
  addSubcommand(listOrderCommand)

  val publishCommand = new Subcommand("publish") {
    val waitOrder = opt[Boolean]("wait", 'w', descr = "wait")
    val objectName = trailArg[String](name = "Object Name")
    val location = trailArg[String](name = "Publush location")
  }
  addSubcommand(publishCommand)

  val purgeCommand = new Subcommand("purge") {
    val dryRun = opt[Boolean]("dryrun", descr = "Do dry run", noshort = true)
    val orderId = trailArg[String](name = "OrderId")
  }
  addSubcommand(purgeCommand)

  val discardCommand = new Subcommand("discard") {
    val objectNames = trailArg[List[String]](name = "Object Names")
  }
  addSubcommand(discardCommand)

  verify()
}


object Weave  {
  val banner = """
 ___       __   _______   ________  ___      ___ _______
|\  \     |\  \|\  ___ \ |\   __  \|\  \    /  /|\  ___ \
\ \  \    \ \  \ \   __/|\ \  \|\  \ \  \  /  / | \   __/|
 \ \  \  __\ \  \ \  \_|/_\ \   __  \ \  \/  / / \ \  \_|/__
  \ \  \|\__\_\  \ \  \_|\ \ \  \ \  \ \    / /   \ \  \_|\ \
   \ \____________\ \_______\ \__\ \__\ \__/ /     \ \_______\
    \|____________|\|_______|\|__|\|__|\|__|/       \|_______|

"""

  def orderMain(conf: WeaveConf): Unit = {
    println(banner)

    val planner = new Planner(Path.fromString(conf.config()), conf.debug())
    if (conf.orderCommand.relaxCheckPoint()) {
      println(scala.Console.RED + "Allowing inconsistent checkpoint may results serious inconsistency of experimental results" + scala.Console.RESET)
      println(scala.Console.RED + "DO NOT COMPUTE ANY RESULTS WITH THIS FLAG" + scala.Console.RESET)
      if (scala.io.StdIn.readLine("Continue [y/N]: ") != "y") {
        println("Quitted")
        System.exit(0)
      }
    }
    planner.makePlan(
      jars=conf.jars(),
      names=conf.orderCommand.objects(),
      redo=conf.orderCommand.redo(),
      dryRun=conf.orderCommand.dryRun(),
      strictCheckPoint=(! conf.orderCommand.relaxCheckPoint()),
      ignoreCheckPoint=conf.orderCommand.ignoreCheckPoint())

    System.exit(0)
    // This is required for avoid JVM behaviour that doesn't terminate
    // after launching a child process
  }

  def execMain(conf: WeaveConf): Unit = {
    implicit val logger = Logger(classOf[Launcher])
    val launcher = new Launcher(Path.fromString(conf.config()))
    launcher.launch(conf.execCommand.launchInfo())
    System.exit(0)
  }

  def storeLogMain(conf: WeaveConf): Unit = {
    val configPath = Path.fromString(conf.config())
    val launcherConfig = Json.parse(configPath.string).asInstanceOf[JsObject]
    val storage =
      Storage.createStorage((launcherConfig \ "storage").as[JsObject])

    val launchInfoPath = conf.storeLogCommand.launchInfo()
    val launchInfo = Json.parse(launchInfoPath.string).asInstanceOf[JsObject]
    val outputKey = (launchInfo \ "outputKey").as[String]

    storage.storeLog(outputKey, LogType_Output, conf.storeLogCommand.outLog())
    storage.storeLog(outputKey, LogType_Error, conf.storeLogCommand.errLog())
  }

  def parseStatusFilter(f: String): ((JobStatus, String, String) => Boolean) = {
    val hex = "[0-9A-Fa-f]+".r
    f match {
      case "failed" =>
        ((js: JobStatus, digest: String, sign: String) => js.isFailed)
      case "running" =>
        ((js: JobStatus, digest: String, sign: String) => js.isRunning)
      case "pending" =>
        ((js: JobStatus, digest: String, sign: String) => js.isPending)
      case "completed" =>
        ((js: JobStatus, digest: String, sign: String) => js.isCompleted)
      case hex() =>
        ((js: JobStatus, digest: String, sign: String) =>
          digest.startsWith(f))
      case _ =>
        ((js: JobStatus, digest: String, sign: String) =>
          sign.indexOf(f) >= 0)
    }
  }

  /**
    * Entry point for status command
    */
  def statusMain(conf: WeaveConf): Unit = {
    val config = Json.parse(Path.fromString(conf.config()).string).asInstanceOf[JsObject]
    val journal = Journal.createJournal((config \ "journal").as[JsObject])
    val scheduler = Scheduler.createScheduler((config \ "scheduler").as[JsObject])
    val schedSnapshot = scheduler.getSnapshot()

    val termWidth = jline.TerminalFactory.get().getWidth()
    val storage =
      Storage.createStorage((config \ "storage").as[JsObject])
    val storageKeySet = storage.keySet

    val orderId = conf.statusCommand.orderId.toOption.orElse {
      journal.lastOrderId
    } match {
      case Some(s) => s
      case None => throw new RuntimeException("No order specified nor found")
    }

    val jobinfos = journal.getJobs(orderId)

    val statuses = {
      for (job <- jobinfos) yield {
        val outputExists = storageKeySet.exists(job.outputKey)
        val schedStatusOpt = schedSnapshot.status(job.jobId)

        if (schedStatusOpt.isEmpty) {
          if (outputExists) {
            "Completed"
          } else {
            println(s"not scheduled, and no object found ${job.jobId}")
            "Lost"
          }
        } else {
          val schedStatus = schedStatusOpt.get;
          if (outputExists) {
            "Completed"
          } else if (schedStatus.isRunning) {
            "Running"
          } else if (schedStatus.isFailed) {
            "Failed"
          } else if (schedStatus.isPending) {
            "Pending"
          } else if (schedStatus.isCompleted) {
            "Completed"
          } else {
            println(s"unknown status ${schedStatus}")
            "Lost"
          }
        }
      }
    }


    val locViewWidth = scala.math.min(50, statuses.size)
    val locViewRunning = Array.fill(locViewWidth)(" ")
    val locViewFailed = Array.fill(locViewWidth)(" ")
    val locViewPending = Array.fill(locViewWidth)(" ")
    val locViewCompleted = Array.fill(locViewWidth)(" ")
    val locViewLost = Array.fill(locViewWidth)(" ")

    for ((st, loc) <- statuses.zipWithIndex) {
      val x = ((loc / statuses.size.toDouble) * locViewWidth).toInt
      if (st == "Completed") {
        locViewCompleted(x) = "C"
      } else if (st == "Running") {
        locViewRunning(x) = "R"
      } else if (st == "Pending") {
        locViewPending(x) = "P"
      } else if (st == "Failed") {
        locViewFailed(x) = "F"
      } else if (st == "Lost") {
        locViewLost(x) = "?"
      }
    }

    println(s" Status of Order [${orderId}]")
    val locViewCompletedStr = locViewCompleted.mkString("")
    val locViewRunningStr = locViewRunning.mkString("")
    val locViewFailedStr = locViewFailed.mkString("")
    val locViewPendingStr = locViewPending.mkString("")
    val locViewLostStr = locViewLost.mkString("")
    println(s" Completed: ${locViewCompletedStr}: ${statuses.count(_ == "Completed")}")
    println(s"   Running: ${locViewRunningStr}: ${statuses.count(_ == "Running")}")
    println(s"   Pending: ${locViewPendingStr}: ${statuses.count(_ == "Pending")}")
    if (statuses.count(_ == "Failed") > 0) {
      println(scala.Console.RED +
        s"    Failed: ${locViewFailedStr}: ${statuses.count(_ == "Failed")}" +
        scala.Console.RESET)
    }
    if (statuses.count(_ == "Lost") > 0) {
      println(scala.Console.RED +
        s"      Lost: ${locViewLostStr}: ${statuses.count(_ == "Lost")}" +
        scala.Console.RESET)
    }

    val launchInfos = jobinfos.map(j => Json.parse(j.launchInfo))

    println(" === Running jobs ===")
    for {
      (launchInfo, st) <- launchInfos zip statuses;
      if st == "Running"
    } {
      val dig = (launchInfo \ "outputKey").as[String].substring(0, 8)
      val s = Prettifier.prettifyFunctionSignature(
        (launchInfo \ "signature").as[String])
      println(s"- ${dig} ${s}")
      val trace = (launchInfo \ "trace").asOpt[Seq[String]].getOrElse(Seq())
      for (tr <- trace) {
        println(s"    ${tr}")
      }
    }

    println(" === Failed jobs ===")
    if (statuses.count(_ == "Failed") == 0) {
      println(" No failed jobs :)");
    } else {
      for {
        (launchInfo, st) <- launchInfos zip statuses;
        if st == "Failed"
      } {
        val dig = (launchInfo \ "outputKey").as[String].substring(0, 8)
        val s = Prettifier.prettifyFunctionSignature(
          (launchInfo \ "signature").as[String])
        println(s"- ${dig} ${s}")
        val trace = (launchInfo \ "trace").asOpt[Seq[String]].getOrElse(Seq())
        for (tr <- trace) {
          println(s"    ${tr}")
        }
      }
    }



    System.exit(0)
    // This is required for avoid JVM behaviour that doesn't terminate
    // after launching a child process
  }

  def resolveObjectKey(planner: Planner, jars: Seq[String], objKeyOrName: String)
      : (ClassLoader, String) = {
    val hex = "[0-9A-Fa-f]+".r
    objKeyOrName match {
      case hex() => {
        val cl = Eval.classLoaderWithJarPaths(
          jars.map { jar => Path.fromString(jar) })
        val keys = planner.storage.complete(objKeyOrName)
        if (keys.size == 0) {
          throw new RuntimeException(s"Key ${objKeyOrName} not found")
        } else if (keys.size > 1) {
          throw new RuntimeException(s"Key ${objKeyOrName} is ambiguous (${keys.size} objects found)")
        } else {
          (cl, keys.toSeq.head)
        }
      }
      case _ => {
        val (cl, jarKeys, plans) =
          planner.constructPlan(jars = jars, names = Seq(objKeyOrName))
        (cl, plans.flatten.head.spec.digest)
      }
    }
  }

  def waitOrder(storage: Storage, objKey: String) {
    val waitPollInterval = 10000  // sleep 10 sec
    print("Waiting orders...")
    while (! storage.exists(objKey)) {
      Thread.sleep(waitPollInterval)
      print(".")
    }
    println("done")
  }

  def showMain(conf: WeaveConf): Unit = {
    val planner = new Planner(Path.fromString(conf.config()))
    val (classLoader, objKey) =
      resolveObjectKey(planner, conf.jars(), conf.showCommand.objectName())
    if (conf.showCommand.waitOrder()) waitOrder(planner.storage, objKey)
    Console.err.println(s"Showing ${objKey}")
    val serializer = new Serializer(planner.storage, classLoader)
    serializer.loadObject(objKey).show()
  }

  def showLogMain(conf: WeaveConf): Unit = {
    import sys.process._
    val pagerBin = sys.env.getOrElse("PAGER", "more")

    val planner = new Planner(Path.fromString(conf.config()))
    val objKeys = (for (kp <- conf.showLogCommand.objectNames()) yield {
      if (kp.length == 40) {
        Seq(kp)
      } else {
        val comp = planner.storage.complete(kp, true).toSeq
        if (comp.size == 0) {
          println(s"  Log file starting with ${kp} is not found. The process might be interrupted before storing logs")
        }
        comp
      }
    }).flatten

    for (objKey <- objKeys) {
      println(s"Showing stdout of ${objKey}")
      planner.storage.loadLog(objKey, LogType_Output) match {
        case Some(p) => {
          (Seq("cat", p.path) #| Seq("sh", "-c", pagerBin + " > /dev/tty")).!;
        }
        case None => {
        }
      }
      println(s"Showing stderr of ${objKey}")
      planner.storage.loadLog(objKey, LogType_Error) match {
        case Some(p) => {
          (Seq("cat", p.path) #| Seq("sh", "-c", pagerBin + " > /dev/tty")).!;
        }
        case None => {
        }
      }
    }
  }

  def publishMain(conf: WeaveConf): Unit = {
    val planner = new Planner(Path.fromString(conf.config()))
    val (classLoader, objKey) =
      resolveObjectKey(planner, conf.jars(), conf.publishCommand.objectName())
    if (conf.publishCommand.waitOrder()) waitOrder(planner.storage, objKey)
    val serializer = new Serializer(planner.storage, classLoader)
    val obj = serializer.loadObject(objKey)
    serializer.exportObject(planner.publisher, conf.publishCommand.location(), obj)
  }

  def purgeMain(conf: WeaveConf): Unit = {
    val config = Json.parse(Path.fromString(conf.config()).string).asInstanceOf[JsObject]
    val journal = Journal.createJournal((config \ "journal").as[JsObject])
    val storage =
      Storage.createStorage((config \ "storage").as[JsObject])

    val jobinfos = journal.getJobs(conf.purgeCommand.orderId())
    val outputKeys =
      jobinfos.map(j => Json.parse(j.launchInfo)).map(j => (j \ "outputKey").as[String])

    for (outputKey <- outputKeys) {
      if (conf.purgeCommand.dryRun()) {
        println(s"Deleting ${outputKey}... (dry run)")
      } else {
        print(s"Deleting ${outputKey}... ")

        storage.deleteObject(outputKey)
        println("(done)")
      }

    }

  }

  def listOrderMain(conf: WeaveConf): Unit = {
    val configFilePath = Path.fromString(conf.config())
    val config = Json.parse(configFilePath.string).as[JsObject]
    val journal = Journal.createJournal((config \ "journal").as[JsObject])
    val n = conf.listOrderCommand.number()
    println(s"\tOrder ID                            \tTimestamp")
    println(s"-----------------------------------------------------------------------------")
    for ((order, i) <- journal.selectLatestOrders(n).zipWithIndex) {
      val timestamp = W3CDTF.format(order.orderedAt, None)
      println(s"""[${i + 1}]\t${order.orderId}\t${timestamp}
\t${order.digest.take(8)}\t${order.name}""")
    }
  }

  def discardMain(conf: WeaveConf): Unit = {
    val planner = new Planner(Path.fromString(conf.config()))
    val objKeys = (for (kp <- conf.discardCommand.objectNames()) yield {
      if (kp.length == 40) {
        Seq(kp)
      } else {
        planner.storage.complete(kp, true).toSeq
      }
    }).flatten

    objKeys.par.foreach { objKey =>
      println(s"Deleting ${objKey}...")
      planner.storage.deleteObject(objKey)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new WeaveConf(args)
    conf.subcommand match {
      case Some(conf.orderCommand) => orderMain(conf)
      case Some(conf.execCommand) => execMain(conf)
      case Some(conf.storeLogCommand) => storeLogMain(conf)
      case Some(conf.showCommand) => showMain(conf)
      case Some(conf.publishCommand) => publishMain(conf)
      case Some(conf.listOrderCommand) => listOrderMain(conf)
      case Some(conf.showLogCommand) => showLogMain(conf)
      case Some(conf.statusCommand) => statusMain(conf)
      case Some(conf.purgeCommand) => purgeMain(conf)
      case Some(conf.discardCommand) => discardMain(conf)
      case _ => {
      }
    }
  }
}
