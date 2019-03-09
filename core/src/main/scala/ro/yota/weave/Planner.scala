package ro.yota.weave

import java.io.File
import scalax.file.Path
import scalax.file.ImplicitConversions._
import play.api.libs.json._
import play.api.libs.json.Json._
import com.typesafe.scalalogging.Logger

import scala.language.postfixOps
import ro.yota.weave.storage.{Storage,StorageKeySet}
import scala.util.matching.Regex.Match

/**
  * PlanStatus is an internal representation of task of each plan
  * (object generation)
  */
trait PlanStatus {
  def name: String
  def shortName: String
}

case object PlanStatus_New extends PlanStatus {
  val name = "new"
  val shortName = "*"
}
case object PlanStatus_Pending extends PlanStatus {
  val name = "pending"
  val shortName = "P"
}
case object PlanStatus_Running extends PlanStatus {
  val name = "running"
  val shortName = "R"
}
case object PlanStatus_Completed extends PlanStatus {
  val name = "completed"
  val shortName = "."
}
case object PlanStatus_Failed extends PlanStatus {
  val name = "failed"
  val shortName = "F"
}
case object PlanStatus_Unknown extends PlanStatus {
  val name = "unknown"
  val shortName = "?"
}
case object PlanStatus_CheckPointed extends PlanStatus {
  val name = "stored"
  val shortName = "S"
}

/**
  * The object listens instantiation of all new plan
  */
trait PlanListener {
  /**
    * Callback function called when plan is instantiated
    */
  def newPlanInstantiated(v: AnyPlan)
}

class NullPlanListener extends PlanListener {
  def newPlanInstantiated(v: AnyPlan) {}
}

/**
  * This class is for providing reverse dependency graph
  *
  * Since each plan only has dependency objects, this listener is required
  *  for tracking reverse dependencies.
  */
class ReverseDependencyTracker extends PlanListener {
  val provideMap: scala.collection.mutable.HashMap[String, Seq[AnyPlan]] =
    new scala.collection.mutable.HashMap[String, Seq[AnyPlan]]()
  val selfMap: scala.collection.mutable.HashMap[String, AnyPlan] =
    new scala.collection.mutable.HashMap[String, AnyPlan]()

  /**
    * Return a list of plans that uses the specified plan
    */
  def usedIn(p: AnyPlan): Seq[AnyPlan] = {
    provideMap.getOrElse(p.spec.digest, Seq())
  }

  def newPlanInstantiated(v: AnyPlan): Unit = {
    selfMap += (v.spec.digest -> v)
    for (dep <- v.spec.depends) {
      if (provideMap contains dep.spec.digest) {
        provideMap += (dep.spec.digest -> (provideMap(dep.spec.digest) ++ Seq(v)))
      } else {
        provideMap += (dep.spec.digest -> Seq(v))
      }
    }
  }

}


object Planner {
  /**
    *  Rewrite config json file to use debug scheduler
    */
  def rewriteConfigForDebugMode(conf: JsObject): JsObject = {
    val schConf = (conf \ "scheduler").as[JsObject]
    val newSchConf =
      schConf ++ Json.obj(
        "className" -> "ro.yota.weave.DebugScheduler",
        "isInteractive" -> true
      )
    conf ++ Json.obj("scheduler" -> newSchConf)
  }
}

/**
  * Planner traverses recipes and identifies which objects have to be generated
  */
class Planner(val plannerConfig: JsObject) {

  private[this] val logger = Logger(classOf[Planner])

  def this(configFilePath: Path, debug: Boolean = false) {
    this(
      if (debug) {
        Planner.rewriteConfigForDebugMode(Json.parse(configFilePath.string).as[JsObject])
      } else {
        Json.parse(configFilePath.string).as[JsObject]
      }
    )
  }

  val storage = Storage.createStorage((plannerConfig \ "storage").as[JsObject])

  val journal = Journal.createJournal((plannerConfig \ "journal").as[JsObject])

  val scheduler = Scheduler.createScheduler((plannerConfig \ "scheduler").as[JsObject])

  val publisher = (plannerConfig \ "publishers").asOpt[JsArray] match {
    case Some(conf) => Publisher.createPublisher(conf)
    case None => Publisher.createDefaultPublisher()
  }

  def planStatusFromJobStatus(stat: JobStatus): PlanStatus = {
    if (stat.isPending) {
      PlanStatus_Pending
    } else if (stat.isRunning) {
      PlanStatus_Running
    } else if (stat.isFailed) {
      PlanStatus_Failed
    } else {
      PlanStatus_Unknown
    }
  }

  def checkPlanStatus(plan: AnyPlan,
    keySetOpt: Option[StorageKeySet] = None,
    schedSnapshot: Option[SchedulerSnapshot] = None,
    strictCheckPoint: Boolean = true)
      : PlanStatus = {
    val storageCheck = keySetOpt match {
      case Some(keySet) => keySet.exists(plan.spec.digest)
      case None => storage.exists(plan.spec.digest)
    }
    if (storageCheck) {
      PlanStatus_Completed
    } else if (plan.isCheckPointed(publisher, strictCheckPoint)) {
      PlanStatus_CheckPointed
    } else {
      val statOpt = schedSnapshot.map(ss => ss.status(plan.spec.digest)).getOrElse {
        scheduler.status(plan.spec.digest)
      }
      statOpt.map(planStatusFromJobStatus).getOrElse(PlanStatus_New)
    }
  }

  def importCheckPoint(plan: AnyPlan, serializer: Serializer) {
    val cpStat = plan.checkPointStatus(publisher)
    if (! cpStat.exists(_.found)) {
      // Logic error, this shouldn't happen
      throw new RuntimeException(s"Checkpoint not found for ${plan.spec.digest}")
    } else {
      val location = if (cpStat.exists(_.consistent)) {
        cpStat.filter(_.consistent).head.location
      } else {
        val pairs = cpStat.map({
          case CheckPointInconsistent(loc, dig) => Some((loc, dig))
          case _ => None
        }).filterNot(_ isEmpty).map(_ get)

        val locations = pairs.map(_._1)
        val digests = pairs.map(_._2)

        println(s"Import from inconsistent checkpoint at ${locations.head}")
        println(s"There're following inconsistent checkpoints")
        for ((loc, dig) <- locations.zip(digests)) {
          println(s"  - ${dig.substring(0,8)} ${loc}")
        }
        locations.head
      }
      val obj = serializer.importObject(publisher, location)
      serializer.storeObject(plan.spec.digest, obj, Json.obj())
    }
  }

  def traversePlans(serializer: Serializer, keySet: StorageKeySet, //scalastyle:ignore
    schedSnapshot: SchedulerSnapshot,
    planned: Seq[AnyPlan], plannedKeys: Set[String],
    toOrder: AnyPlan, redo: Boolean, strictCheckPoint: Boolean,
    ignoreCheckPoint: Boolean)
      : (Seq[AnyPlan], Set[String]) = {
    val planSt = checkPlanStatus(toOrder, Some(keySet), Some(schedSnapshot),
      strictCheckPoint)
    val exist =
      if (redo) { false }
      else if (planSt == PlanStatus_CheckPointed) {
        if (ignoreCheckPoint) {
          false
        } else {
          importCheckPoint(toOrder, serializer)
          true
        }
      } else {
        planSt != PlanStatus_New
      }

    if ((plannedKeys contains toOrder.spec.digest) || exist) {
      (planned, plannedKeys)
    } else {
      val (newPlanned, newPlannedKeys) =
        toOrder.spec.depends.foldLeft((planned,plannedKeys)) {
          case ((planned, plannedKeys), spec) => {
            traversePlans(serializer, keySet, schedSnapshot,
              planned, plannedKeys, spec, false,
              strictCheckPoint, ignoreCheckPoint)
          }
        }
      (newPlanned :+ toOrder, newPlannedKeys + toOrder.spec.digest)
    }
  }

  /**
    * Convert the dynamically evaluated object into seq of plans
    *
    * If an evaluated object is sequence, this does nothing,
    * if the object is single plan, this constructs single element sequence.
    */
  def resolveCollection(objs: Seq[Object]): Seq[Seq[AnyPlan]] = {
    {
      for(obj <- objs) yield {
        obj match {
          case p: AnyPlan => Seq(p)
          case seq: Seq[_] => {
            seq.map(_ match {
              case (p: AnyPlan) => p
              case _ =>
                throw new RuntimeException("An element of collection is not a Plan")
            })
          }
          case _ => {
            throw new RuntimeException(s"${obj.getClass.toString} cannot be casted to AnyPlan")
          }
        }
      }
    }
  }

  /**
    * Retrieve checkpoint prefix written in the config file
    */
  def checkPointRootInConfig : Option[String] = {
    (plannerConfig \ "planner" \ "checkPointRoot").asOpt[String]
  }

  /**
    * Retrieve checkpoint level written in the config file
    */
  def checkPointLevelInConfig : Option[Int] = {
    val v = Map(
      "nothing" -> CheckPointLevel.Nothing,
      "important" -> CheckPointLevel.Important,
      "result" -> CheckPointLevel.Result,
      "intermediateresult" -> CheckPointLevel.IntermediateResult,
      "intermediatedata" -> CheckPointLevel.IntermediateData,
      "everything" -> CheckPointLevel.Everything
    )
    val digits = "[0-9]+".r
    for {
      s <- (plannerConfig \ "planner" \ "checkPointLevel").asOpt[String];
      v <- s match {
        case digits() => Some(s.toInt)
        case _ => v.get(s.toLowerCase())
      }
    } yield v
  }

  /**
    * Retrieve config vars defined in the config file
    */
  def configVarsInConfig : Map[String, JsValue] = {
    (plannerConfig \ "planner" \ "configVars").asOpt[Map[String, JsValue]].getOrElse(Map())
  }



  /**
    * Construct Plan objects from the given variable names and jars
    *
    * Hook can be used for tracking recipe evaluation
    */
  def constructPlan(
    jars: Seq[String], names: Seq[String],
    uploadJars: Boolean = true)
      : (ClassLoader, Seq[String], Seq[Seq[AnyPlan]]) = {
    val jarPaths = jars.map { jar => Path.fromString(jar) }
    val jarKeys = if (uploadJars) {
      jars.map { jar => storage.storeJar(jar) }
    } else {
      jars.map { jar => Hasher(jar) }
    }
    val cl = Eval.classLoaderWithJarPaths(jarPaths)

    val defaultContext = ProjectContextStack.current()
    val context = defaultContext.copy(
      checkPointPrefix = Seq(checkPointRootInConfig.getOrElse("/")),
      checkPointLevel = checkPointLevelInConfig.getOrElse(defaultContext.checkPointLevel),
      configVars = defaultContext.configVars ++ configVarsInConfig
    )

    ProjectContextStack.pop()
    ProjectContextStack.push(context)

    (cl, jarKeys, resolveCollection(Eval.evalMany[Object](cl, names)))
  }

  def filterNewPlans(plans: Seq[AnyPlan], keySet: StorageKeySet,
    schedSnapshot: SchedulerSnapshot, ignoreCheckPoint: Boolean,
    redo: Boolean,
    serializer: Serializer): Seq[AnyPlan] = {
    (for (plan <- plans) yield {
      val st = checkPlanStatus(plan, Some(keySet), Some(schedSnapshot))
      st match {
        case PlanStatus_Unknown => {
          throw new RuntimeException(
            s"Cannot obtain valid plan status for ${plan.spec.digest}");
        }
        case PlanStatus_New => Some(plan)
        case _ if redo => {
          Some(plan)
        }
        case _ => {
          // Failed/ Running/ Completed/ Pending plan is just ignored if it's ordered
          None
        }
      }
    }).flatten
    // Tree traversal and topological sort
  }

  /**
    * Traverse dependency of "plansRaw" depending on status of the objects
    *
    * This method goes as follows:
    *  1. Resolve roots of "plansRaw" if one of those is field accessor
    *     similar objects that don't have corresponding job
    *  2. traverse
    */
  def schedulePlan(serializer: Serializer,
    newPlans: Seq[AnyPlan],
    keySet: StorageKeySet, schedSnapshot: SchedulerSnapshot,
    redo: Boolean,
    strictCheckPoint: Boolean,
    ignoreCheckPoint: Boolean): (Seq[AnyPlan], Seq[AnyPlan]) = {
    // ^ Returns plans that are actually scheduled
    val (plannedPlans, plannedPlansKeys) =
      newPlans.foldLeft((Seq[AnyPlan](), Set[String]())) {
        case ((planned, plannedKeys), plan) => {
          traversePlans(serializer, keySet, schedSnapshot, planned, plannedKeys,
            plan, redo, strictCheckPoint, ignoreCheckPoint)
        }
      }
    val plannedConsts = plannedPlans.filter(_ isConstant)
    val plannedJobs = plannedPlans
      .filterNot(_.isConstant).filter(_.isInstanceOf[Launchable])
    (plannedConsts, plannedJobs)
  }

  def writeConstants(classLoader: ClassLoader, plannedConsts: Seq[AnyPlan]) {
    val serializer = new Serializer(this.storage, classLoader)
    println(s"${plannedConsts.length} constants to be written now")

    plannedConsts.par.foreach {
      case constproc: Procedure[_] => {
        logger.info(s"K ${constproc.spec.shortDigest} ${constproc.prettyPrint(TermUI.termWidth - 12)}")
        val v = constproc.function.launch(serializer, Seq())
        serializer.storeObject(constproc.spec.digest, v, constproc.launchInfo)
      }
      case const => {
        logger.info(s"${const.prettyPrint(TermUI.termWidth - 12)} is not launchable")
      }
    }
  }

  def enqueueJobs(jarKeys: Seq[String], plannedJobs: Seq[AnyPlan],
    orderId: Option[String] = None) {
    val schedSnapshot = scheduler.getSnapshot()
    println(s"${plannedJobs.length} operations planned")
    val keySet = storage.keySet
    for (job <- plannedJobs) {
      val st = checkPlanStatus(job, Some(keySet), Some(schedSnapshot))
      val depKeys = job.spec.depends.map(_.spec.shortDigest)
      println(s"${st.shortName} ${job.spec.shortDigest} ${job.prettyPrint(TermUI.termWidth - 12)}")
    }

    if (plannedJobs.size > 0) {
      val oid: String = orderId.getOrElse("")
      val infos = scheduler.schedule(this, plannedJobs, jarKeys = jarKeys)
      journal.writeNewJobs(oid,
        for ((job, info) <- (plannedJobs zip infos)) yield {
          val launchInfo = job.launchInfoOpt
            .map(Json.stringify(_))
            .getOrElse("")
          (job.spec.shortDigest, launchInfo, Json.stringify(info))
        }
      )
    }

  }

  /**
    * Entry point of "order" command
    *
    * @param jars Sequence of strings for paths or digest prefixes of jars
    * @param names Object names to be ordered
    * @param redo Redo flag for regenerating existing objects
    * @param dryRun If true, do dry-run
    * @param strictCheckPoint
    *    If true, checkpoints with inconsistent digests will not be used
    * @param ignoreCheckPoint
    *    If true, checkpoints will not be used
    */
  def makePlan(jars: Seq[String], names : Seq[String], redo: Boolean,
    dryRun: Boolean,
    strictCheckPoint: Boolean,
    ignoreCheckPoint: Boolean) {

    import scala.concurrent._
    import scala.concurrent.duration._
    implicit val ec = ExecutionContext.global

    val keySetFuture = Future { storage.keySet }
    val schedSnapshotFuture = Future { scheduler.getSnapshot() }

    val (cl, jarKeys, plans)  =
      Spinner.withSpinning("evaluating projects...") {
        constructPlan(jars, names, uploadJars = ! dryRun)
      }

    println("Ordered products:")
    for (planseq <- plans) {
      for (plan <- planseq) {
        println(s"- ${plan.spec.shortDigest} ${plan.prettyPrint(TermUI.termWidth - 12)}")
      }
    }

    val serializer = new Serializer(storage, cl)

    val (keySet, schedSnapshot) =
      Spinner.withSpinning("taking snapshot of scheduler and storage...") {
        (Await.result(keySetFuture, Duration.Inf),
          Await.result(schedSnapshotFuture, Duration.Inf))
      }

    val newPlans = filterNewPlans(plans.flatten.map(_.root), keySet, schedSnapshot, ignoreCheckPoint, redo, serializer)
    println(s"Following ${newPlans.size} plans need to be completed:")
    for (plan <- newPlans) {
      println(s"* ${plan.spec.shortDigest} ${plan.prettyPrint(TermUI.termWidth - 12)}")
    }

    val (plannedConsts, plannedJobs) =
      Spinner.withSpinning("planning...") {
        schedulePlan(
          serializer, newPlans, keySet, schedSnapshot,
          redo, strictCheckPoint, ignoreCheckPoint)
      }

    if (! dryRun) {
      writeConstants(cl, plannedConsts)
      val orderId = java.util.UUID.randomUUID.toString

      if (redo) { // Remove the products before enqueing the jobs
        for (job <- plannedJobs) {
          if (keySet.exists(job.spec.digest)) {
            storage.deleteObject(job.spec.digest)
          }
          // This actually changes key set, but considering that deletion
          // happening at the very end of planning, it's not necessary to
          // reflect it to the keySet
        }
      }

      enqueueJobs(jarKeys, plannedJobs, Some(orderId))

      val pairs = for ((planSeq, name) <- (plans zip names)) yield {
        for (plan <- planSeq) yield {
          (name, plan.spec.digest): (String, String)
        }
      }

      journal.writeOrders(orderId, storage.id, pairs.flatten.unzip._1, pairs.flatten.unzip._2, jars)
      println(s"Order-ID ${orderId} is ordered")
    }
  }

  final val productPollIntervalMSec: Int = 3000

  def wait(waitFor: Seq[AnyPlan]): Unit = {
    var allResolved = false
    while (! allResolved) {
      Thread.sleep(productPollIntervalMSec)
      allResolved = {for (plan <- waitFor) yield {
        checkPlanStatus(plan) match {
          case PlanStatus_Completed => true
          case PlanStatus_Failed => true
          case _ => false
        }
      }}.fold(true)(_ & _)
    }
  }
}
