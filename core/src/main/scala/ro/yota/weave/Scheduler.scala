package ro.yota.weave

import scalax.file.Path
import scalax.file.ImplicitConversions._
import play.api.libs.json._
import play.api.libs.json.Json._
import java.io.File
import sys.process._

import scala.language.postfixOps

trait JobStatus {
  def isRunning: Boolean
  def isPending: Boolean
  def isFailed: Boolean
  def isCompleted: Boolean
  def shortName: String = "?"
}

trait SchedulerSnapshot {
  def targetSpecDigest(key: String): Option[String]
  def taskName(key: String): Option[String]
  def status(key: String): Option[JobStatus]
  def keys: Iterator[String]
}

/**
  * Abstract base class for hiding implementation of scheduling back-end
  */
abstract class Scheduler(schedulerConfig: JsObject) {
  val scriptEnvVars: Seq[(String, String)] =
    (schedulerConfig \ "scriptEnvVars")
      .asOpt[Seq[Seq[String]]].getOrElse(List())
      .map { l =>
      if (l.length != 2) {
        throw new RuntimeException("Element of scriptEnvVars must have 2 elements")
      } else {
        (l(0), l(1))
      }
    }

  val envVarSettingScript = this.scriptEnvVars.map { case (key, value) =>
    s"export ${key}=" + "\"" + value + "\"\n"
  }.mkString("\n")

  def status(key: String) : Option[JobStatus]
  def schedule(planner: Planner, plans: Seq[AnyPlan], jarKeys: Seq[String],
    dryRun: Boolean = false, debugLaunch: Boolean = false) : Seq[JsObject]

  def getSnapshot() : SchedulerSnapshot;
}

object Scheduler {
  def createScheduler(schedulerConfig : JsObject) : Scheduler = {
    val className = (schedulerConfig \ "className").as[String]
    val ctr = Class.forName(className).getConstructor(classOf[JsObject])
    ctr.newInstance(schedulerConfig).asInstanceOf[Scheduler]
  }
}

class AkkaJobStatus() extends JobStatus {
  def isRunning: Boolean = false
  def isPending: Boolean = false
  def isFailed: Boolean = false
  def isCompleted: Boolean = false
}

object AkkaJobStatus {
    def fromString(s: String) : AkkaJobStatus = s match {
    case "pending" => AkkaJobStatus_Pending
    case "running" => AkkaJobStatus_Running
    case "completed" => AkkaJobStatus_Completed
    case "failed" => AkkaJobStatus_Failed
    case "waiting" => AkkaJobStatus_Pending
    case _ => AkkaJobStatus_Unknown
  }
}

case object AkkaJobStatus_Running extends AkkaJobStatus {
  override val isRunning = true
  override val shortName = "R"
}
case object AkkaJobStatus_Pending extends AkkaJobStatus {
  override val isPending = true
  override val shortName = "P"
}
case object AkkaJobStatus_Failed extends AkkaJobStatus {
  override val isFailed = true
  override val shortName = "F"
}
case object AkkaJobStatus_Completed extends AkkaJobStatus {
  override val isCompleted = true
  override val shortName = "."
}
case object AkkaJobStatus_Unknown extends AkkaJobStatus {
}

class AkkaSchedulerSnapshot(
  status: Map[String, AkkaJobStatus],
  metaVars: Map[String, Map[String, String]])
    extends SchedulerSnapshot {

  override def toString: String =
    s"""AkkaSchedulerSnapshot {
  status: ${status}
  metaVars: ${metaVars}
}"""

  def targetSpecDigest(key: String): Option[String] =
    Some(key.substring(0, Hasher.SHA1_HEX_LENGTH))

  def taskName(key: String): Option[String] =
    for {
      vars <- metaVars.get(key);
      sig <- vars.get("humanFriendlyName")
    } yield { sig }

  def status(key: String): Option[JobStatus] = status.get(key)
  def keys: Iterator[String] = status.keysIterator
}

object SchedulerUtils {
  def readMetaVariablesFromScript(src: String): Map[String, String] = {
    (for {
      line <- src.split("\n");
      if line.startsWith("#WEAVE: ");
      eqpair = line.substring("#WEAVE: ".length).trim;
      idx = eqpair.indexOf('=');
      if idx > 0
    } yield {
      (eqpair.substring(0, idx).trim -> eqpair.substring(idx + 1).trim)
    }).toMap
  }
}

class AkkaScheduler(schedulerConfig: JsObject) extends Scheduler(schedulerConfig) {

  val autoRun = (schedulerConfig \ "autoRun").asOpt[Boolean].getOrElse(true)

  def getSnapshot(): AkkaSchedulerSnapshot = {

    val statJson = ro.yota.weaveschd.SchdMain.statusAll().as[Seq[JsObject]]
    val statMap = statJson.map { jobj =>
      val key = (jobj \ "id").as[String]
      key -> AkkaJobStatus.fromString((jobj \ "status").as[String])
    }.toMap
    val metaInfo = statJson.map { jobj =>
      val key = (jobj \ "id").as[String]
      val v = SchedulerUtils.readMetaVariablesFromScript(
        (jobj \ "script").as[String])
      key -> v
    }.toMap
    new AkkaSchedulerSnapshot(statMap, metaInfo)
  }

  def status(key: String): Option[JobStatus] = {
    val statJson = ro.yota.weaveschd.SchdMain.statusAll()
    val statMap = statJson.as[Seq[JsObject]].map { jobj =>
      (jobj \ "id").as[String] ->
      AkkaJobStatus.fromString((jobj \ "status").as[String])
    }.toMap
    if (statMap contains key) {
      Some(statMap(key))
    } else {
      None
    }
  }

  def makeJobName(planner: Planner, spec: Spec): String = {
    spec.digest + "@" + planner.storage.id
  }

  final val schedulerPollIntervalMSec: Int = 1000

  def checkAndRunBackEndIfNecessary(): Unit = {
    if (autoRun) {
      if (! ro.yota.weaveschd.SchdMain.checkAlive) {
        print("Autorun weaveschd scheduler...")
        Seq("weaveschd_start") !;
        Thread.sleep(schedulerPollIntervalMSec)
        while (! ro.yota.weaveschd.SchdMain.checkAlive) {
          print("...")
          Thread.sleep(schedulerPollIntervalMSec)
        }
        println("done.")
      } else {
        println("SCHD is already running.")
      }
    }
  }

  def makeScript(planner: Planner, plan: AnyPlan, launchInfo: String): String = {
    val stringifiedConfig = Json.stringify(planner.plannerConfig)
    """
#WEAVE: humanFriendlyName=""" + plan.spec.signature + """
case $(uname) in
  Darwin)
    SANDBOX=$(mktemp -t "weave" -d)
    ;;
  Linux)
    SANDBOX=$(mktemp -d)
    ;;
esac
CONF=${SANDBOX}/config.json
LAUNCHINFO=${SANDBOX}/launchinfo.json
OUTLOG=${SANDBOX}/stdout.txt
ERRLOG=${SANDBOX}/stderr.txt

cd ${SANDBOX}

cat << EOD > $CONF
""" + stringifiedConfig + """
EOD

cat << EOD > $LAUNCHINFO
""" + launchInfo + """
EOD
""" + envVarSettingScript + """

weave -c $CONF exec $LAUNCHINFO > $OUTLOG 2> $ERRLOG
RET=$?
weave -c $CONF store-log $LAUNCHINFO $OUTLOG $ERRLOG
rm -r ${SANDBOX}
test $RET -eq 0
"""
  }

  /**
    * Find dependency object that is in process, or ordered in the same timing
    *   and returns list of corresponding job names
    */
  def findActiveDependencies(planner: Planner, plan: AnyPlan, digestSet: Set[String],
    snapshot: AkkaSchedulerSnapshot): Seq[String] = {
    (for (dep <- plan.spec.depends.map(_.root)) yield {
      {
        if (digestSet contains dep.spec.digest) {
          Some(makeJobName(planner, dep.spec))
        }
        else { None }
      } orElse { // ^ if the dependent will be created in same order,
                 // ↓ or is created by previous order, and currently working on
        snapshot.status(makeJobName(planner, dep.spec)) match {
          case Some(st) => {
            if (st.isCompleted) {
              None
            } else {
              Some(makeJobName(planner, dep.spec))
            }
          }
          case None => None
        }
      }
    }).filterNot(_ isEmpty).map(_ get)
  }

  /**
    * Put jobs to the backend queue, and returns launch info
    *
    * plans are assumed to be topologically sorted
    */
  def schedule(planner: Planner, plans: Seq[AnyPlan], jarKeys: Seq[String],
    dryRun: Boolean = false, debugLaunch: Boolean = false): Seq[JsObject] = {

    if (! dryRun && ! debugLaunch) { checkAndRunBackEndIfNecessary() }

    // Set of digests to be scheduled in this function call
    val digestSet = plans.map(_.root.spec.digest).toSet
    val snapshot = this.getSnapshot()

    val launchInfos = for (plan <- plans.reverse) yield {
      val jobName = makeJobName(planner, plan.spec)

      // get active dependencies that has corresponding job running or to be
      // executed
      val activeDep = findActiveDependencies(planner, plan, digestSet, snapshot)

      val inputKeys = plan.spec.depends.map(_.spec.digest)
      val outputDigest = plan.spec.digest

      val launchInfo =
        Json.stringify(
          plan.asInstanceOf[Launchable].launchInfo ++
            Json.obj("jarKeys" -> jarKeys)
        )

      Json.obj(
        "jobId" -> jobName,
        "deps" -> activeDep,
        "script" -> makeScript(planner, plan, launchInfo),
        "shell" -> "zsh"
      )
    }
    if (launchInfos.size > 0) {
      if (dryRun) {
        for (jobDesc <- launchInfos) {
          println("==== JOB INFO ====")
          println(jobDesc)
        }
      } else if (debugLaunch) {
        for (jobDesc <- launchInfos) {
          Path.fromString("./" + (jobDesc \ "jobName").as[String] + ".sh")
            .write((jobDesc \ "script").as[String])
        }
      } else {
        ro.yota.weaveschd.SchdMain.submitMany(Json.stringify(JsArray(launchInfos)))
      }
    }
    // The output order needs to be consistent with the input order
    launchInfos.reverse
  }
}

/**
  * Scheduler for debugging
  */
class DebugScheduler(schedulerConfig: JsObject)
    extends Scheduler(schedulerConfig) {
  import scala.util.control.Breaks._


  val isInteractive =
    (schedulerConfig \ "isInteractive").asOpt[Boolean].getOrElse(true)

  def getSnapshot(): SchedulerSnapshot = new SchedulerSnapshot {
    def status(key: String): Option[JobStatus] = None
    def keys: Iterator[String] = Iterator[String]()
    def taskName(key: String): Option[String] = None
    def targetSpecDigest(key: String): Option[String] = None
  }

  def status(key: String) : Option[JobStatus] = None

  def makeScript(configFile: Path, launchInfoFile: Path,
    stdOutFile: Path, stdErrFile: Path): String =
    envVarSettingScript ++ s"""
weave -c ${configFile.path} exec ${launchInfoFile.path} > >(tee ${stdOutFile.path}) 2> >(tee ${stdErrFile.path} >&2)
RET=$$?

weave -c ${configFile.path} store-log ${launchInfoFile.path} ${stdOutFile.path} ${stdErrFile.path}

test $$RET -eq 0
"""

  def execTaskImmediately(plan: AnyPlan, execPath: Path,
    configFile: Path, jarKeys: Seq[String]): (Int, JsObject) = {
    execPath.createDirectory(true, false)

    println(s"Launching ${plan.spec.digest}...")
    val launchInfoObj = plan.asInstanceOf[Launchable].launchInfo ++ Json.obj("jarKeys" -> jarKeys)
    val launchInfo = Json.stringify(launchInfoObj)
    val launchInfoFile = execPath / "launchInfo.json"
    launchInfoFile.write(launchInfo)

    val stdOutFile = execPath / "stdout.txt"
    val stdErrFile = execPath / "stderr.txt"
    val launchScriptFile = execPath / "launch.sh"

    launchScriptFile.write(makeScript(configFile, launchInfoFile,
      stdOutFile, stdErrFile))

    val cmd = Seq("zsh", launchScriptFile.path)
    println(s"Cmd = ${cmd}")
    val retCode = Process(cmd, execPath.fileOption.get).!

    (retCode, launchInfoObj)
  }

  val debugSpaceBasePath = scala.util.Properties.envOrNone("WEAVE_DEBUGSPACE") match {
    case Some(s) => Path.fromString(s)
    case None => {
      val cwd = System.getProperty("user.dir")
      (Path.fromString(cwd) / "weave_dbg")
    }
  }

  def schedule(planner: Planner, plans: Seq[AnyPlan], jarKeys: Seq[String],
    dryRun: Boolean = false, debugLaunch: Boolean = false): Seq[JsObject] = {
    import sys.process._

    val configFile = Path.createTempFile()
    configFile.write(Json.stringify(planner.plannerConfig))

    val ret = new scala.collection.mutable.MutableList[JsObject]()

    breakable {
      for (plan <- plans) {
        val execPath = debugSpaceBasePath / plan.spec.digest

        var skipped = false
        if (execPath.exists) {
          val answer = TermUI.prompt(
            s"${execPath} already exists, delete and continue",
            Seq("continue", "abort", "skip"))
          if (answer == "continue") {
            execPath.deleteRecursively()
          } else if (answer == "abort") {
            break
          } else if (answer == "skip") {
            skipped = true
          }
        }

        if (! skipped) {
          val (retCode, launchInfo) = execTaskImmediately(plan, execPath, configFile, jarKeys)
          ret += launchInfo
          if (retCode == 0) {
            println(s"Clean up ${execPath.path}.")
            execPath.deleteRecursively()
          } else {
            val answer = TermUI.prompt(
              s"Job ${plan.spec.digest} failed",
              Seq("abort", "continue"))
            if (answer == "abort") {
              break
            }
          }
        }
      }
    }
    ret.toSeq
  }
}
