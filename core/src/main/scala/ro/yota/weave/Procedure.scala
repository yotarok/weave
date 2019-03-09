package ro.yota.weave

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import play.api.libs.json._
import play.api.libs.json.Json._
import scalax.io.{Output, Input, Codec}
import scalax.file.Path

import scala.language.implicitConversions
import scala.language.postfixOps
import ro.yota.weave.{planned => p}
import ro.yota.weave.storage.Storage


/**
  * Procedure is a "plan" that has corresponding function (incl. const function)
  *
  * Every plan has a corresponding procedure; i.e. all plans are created by
  *  applying cast/ field-access/ etc. to an output of a function
  *
  * @constructor create a procedure with function and input plans
  * @param function function that processes inputs
  * @param inputs sequence of plans of input objects for the function
  * @param publishingInfo
  *    after the function is computed, the result will be published
  *    according to this parameter
  */
class Procedure[+Ret <: Plannable : TypeTag](
  val function: Function,
  val inputs: Seq[Plan[Plannable]],
  val publishingInfo: Seq[PublishingInfo] = Seq()
) extends Plan[Ret] with Launchable {

  final val signature = function.signature(inputs.map(_.spec.digest))
  final val spec = new ProcedureSpec(signature, inputs)

  // save trace messages
  final val traceMessages = ProjectContextStack.current().traceMessages

  override def prettyPrint(width: Int): String = {
    function.prettyPrint(width, inputs.map(_.spec.shortDigest))
  }

  final val functionClassName: String = function.className

  override val isConstant = function.isConstant

  def isCollapsible: Boolean =
    function.isAuxiliary && publishingInfo.length == 0

  def launchInfo: JsObject = Json.obj(
    "type" -> "Procedure",
    "trace" -> traceMessages,
    "functionClassName" -> functionClassName,
    "signature" -> this.spec.signature,
    "outputKey" -> this.spec.digest,
    "inputKeys" -> this.spec.depends.map(_.spec.digest),
    "checkPoints" -> publishingInfo.map { info =>
      Json.arr(info.keyString, info.fieldNames.mkString("."))
    }
  )

  /**
    * Enable checkpointing of the output of this procedure
    *
    * This method actually returns new procedure that has an additional
    *  publishing info
    */
  def checkPoint(key: String, fieldPaths: Seq[String] = Seq(),
    level: Int = CheckPointLevel.Important): Plan[Ret] = {
    if (ProjectContextStack.current().checkPointLevel >= level) {
      val prefix =
        ProjectContextStack.current().checkPointPrefix.mkString("/") + "/";

      new Procedure[Ret](function, inputs,
        publishingInfo ++
          Seq(PublishingInfo(prefix ++ key, fieldPaths, false)))
    } else {
      this
    }
  }


}

/**
  * CollapsedProcedure is a tree of procedures that can be performed
  * efficiently together
  *
  * Typically collapsed procedure is made by attaching a light-weight input
  * processing procedures to another procedure.
  * A CollapsedProcedure has only one output from the root procedure.
  * The signature of a collapsed procedure is taken from that of the root
  * procedure. However, the spec differs in terms of dependency. A
  * CollapsedProcedure has dependecies to those of leaf nodes.
  *
  * @constructor create a CollapsedProcedure with the given procedures
  * @param procs
  *   Sequence of distinct procedures sorted in execution order
  *   (i.e. post-order).
  * @param rootProc
  *   Root procedure. This must be identical with the last element of procs
  * @param publishingInfo See Procedure for details
  */
class CollapsedProcedure[+Ret <: Plannable : TypeTag](
  val procs: Seq[Procedure[Plannable]],
  val rootProc: Procedure[Ret],
  val publishingInfo: Seq[PublishingInfo] = Seq()
) extends Plan[Ret] with Launchable {

  // verification
  if (rootProc != procs.last) {
    throw new RuntimeException("Root procedure must be the last node of procedure chain")
  }

  private[this] val interimDigests = procs.map(_.spec.digest).toSet
  val inputs = (for (proc <- procs) yield {
    proc.inputs.filter(x => (! (interimDigests contains x.spec.digest)))
  }).flatten.distinct

  final val signature = rootProc.signature
  final val spec = new CollapsedProcedureSpec(rootProc.spec, inputs)

  override def prettyPrint(width: Int): String = {
    val revProcs = procs.reverse
    val head = revProcs.head.function.prettyPrint(
      width, revProcs.head.inputs.map(_.spec.shortDigest))
    val tail = (for (proc <- revProcs.tail) yield {
      s" => ${proc.spec.shortDigest} " + proc.function.prettyPrint(
        width - 12, proc.inputs.map(_.spec.shortDigest))
    }).mkString("\n")
    head + "\n" + tail
  }

  override val isConstant = false

  def isCollapsible: Boolean =
    rootProc.function.isAuxiliary && publishingInfo.length == 0

  def launchInfo: JsObject = Json.obj(
    "type" -> "CollapsedProcedure",
    "signature" -> this.spec.signature,
    "outputKey" -> this.spec.digest,
    "inputKeys" -> this.inputs.map(_.spec.digest),
    "checkPoints" -> publishingInfo.map { info =>
      Json.arr(info.keyString, info.fieldNames.mkString("."))
    },
    "elements" -> Json.arr(
      (procs.map(_.launchInfo : JsValueWrapper)):_*
    )
  )

  /**
    * Enable checkpointing of the output of this procedure
    *
    * This method actually modifies the last procedure in the chain
    *  to include the checkpoint info
    */
  def checkPoint(key: String, fieldPaths: Seq[String] = Seq(),
    level: Int = CheckPointLevel.Important): Plan[Ret] = {

    if (ProjectContextStack.current().checkPointLevel >= level) {
      val nroot = rootProc.checkPoint(key, fieldPaths, level) match {
        case p: Procedure[Ret] => p
        case _ => {
          throw new RuntimeException("The last procedure in the collapsed chain is not a valid procedure")
        }
      }

      val nprocs = procs.init ++ Seq(nroot)

      new CollapsedProcedure[Ret](nprocs, nroot, publishingInfo)
    } else {
      this
    }
  }

}

object Procedure {
  /**
    * Instantiate procedure class
    *
    * If it is collapsible, it generates CollapsedProcedure instead
    */
  def apply[Ret <: Plannable : TypeTag](function: Function,
    inputs : Seq[Plan[Plannable]],
    publishingInfo: Seq[PublishingInfo] = Seq()): Plan[Ret] = {
    // Check collapsibility and collapse if possible
    val collapsible = inputs.exists {
      case proc: Procedure[Plannable] => proc.isCollapsible
      case _ => false
    };
    if (collapsible) {
      val rootProc = new Procedure[Ret](function, inputs, publishingInfo)
      makeCollapsedProcedure(rootProc)
    } else {
      new Procedure[Ret](function, inputs, publishingInfo)
    }
  }

  /**
    * Used for listing collapsible procedures recursively
    */
  private[this] def collectCollapsibles(
    proc: Procedure[Plannable],
    visited: scala.collection.mutable.TreeSet[String])
      : Seq[Procedure[Plannable]] = {
    if (visited contains proc.spec.digest) {
      Seq()
    } else {
      val children = (for (input <- proc.inputs) yield {
        input match {
          case proc: Procedure[Plannable] if proc.isCollapsible => {
            collectCollapsibles(proc, visited)
          }
          case cproc: CollapsedProcedure[Plannable] if cproc.isCollapsible => {
            (for (proc <- cproc.procs) yield { // Assumed to be sorted
              if (visited contains proc.spec.digest) {
                None
              } else {
                visited += proc.spec.digest
                Some(proc)
              }
            }).flatten
          }
          case _ => {
            Seq()
          }
        }
      }).flatten
      visited += proc.spec.digest
      children ++ Seq(proc)
    }
  }

  /**
    * Construct CollapsedProecedure object from the given root procedure
    *
    * @param rootProc Root procedure
    */
  def makeCollapsedProcedure[Ret <: Plannable : TypeTag](
    rootProc: Procedure[Ret]
  ): CollapsedProcedure[Ret] = {
    val visited = scala.collection.mutable.TreeSet[String]()
    val auxProcs: Seq[Procedure[Plannable]] =
      collectCollapsibles(rootProc, visited)
    new CollapsedProcedure(auxProcs, rootProc)
  }

}
