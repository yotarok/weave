package ro.yota.weave

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import play.api.libs.json._
import play.api.libs.json.Json._
import scalax.io.{Output, Input, Codec}
import scalax.file.Path

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.language.postfixOps
import ro.yota.weave.{planned => p}
import ro.yota.weave.storage.Storage

trait CheckPointStatus {
  def location: String
  def found: Boolean
  def consistent: Boolean
}
case class CheckPointConsistent(val location: String) extends CheckPointStatus {
  val found = true
  val consistent = true
}
case class CheckPointInconsistent(
  val location: String, val specDigest: String) extends CheckPointStatus {
  val found = true
  val consistent = false
}
case object CheckPointNotFound extends CheckPointStatus {
  val found = false
  val location = ""
  val consistent = false
}

case class PublishingInfo(
  val keyString: String,
  val fieldNames: Seq[String],
  val writeOnly: Boolean
);

/// define common operations for a plan that are independet of plan type
trait AnyPlan extends Orderable {
  /**
    * Spec object for the plan
    */
  def spec : Spec

  /**
    * LaunchInfo JSON if the plan is launchable, None otherwise
    */
  def launchInfoOpt : Option[JsObject] = None

  def isCheckPointed(publisher: Publisher, strictCheckDigest: Boolean): Boolean = {
    val stats = this.checkPointStatus(publisher)
    if (stats.exists(_.consistent)) {
      true
    } else if (! strictCheckDigest && stats.exists(_.found)) {
      true
    } else {
      false
    }
  }

  def isConstant: Boolean = false

  def root: Launchable;

  /**
    * returns a pretty string for printing to a terminal with width=termWidth
    *
    * By default, it falls back to spec's signature
    */
  def prettyPrint(width: Int): String =
    Prettifier.truncate(spec.signature, width)

  /// See the status of checkpoint corresponding to this plan
  def checkPointStatus(publisher: Publisher): Seq[CheckPointStatus]


  /**
    * Get plan for stdout of the procedure
    *
    * This requires type information so it is implemented in Plan[A]
    */
  def stdout: Plan[p.TextFile]

  /**
    * Get plan for stdout of the procedure
    *
    * This requires type information so it is implemented in Plan[A]
    */
  def stderr: Plan[p.TextFile]

}

abstract class Plan[+A <: Plannable : TypeTag] extends AnyPlan {
  // All plan must have a single procedure that generates the plan
  //def getProcedurePlan: Plan[Plannable];

  def as[B <: planned.Primitive](implicit evA: A <:< planned.Primitive, evB: TypeTag[B]): Plan[B] = {
    Procedure(new Cast[B](), Seq(this, ""))(evB)
  }

  def copy(f: scala.Tuple2[java.lang.String, Plan[Plannable]]*)(implicit ev: A <:< p.Container)
      : Plan[A] = {
    val updates = f.map ({
      case (n, p) =>
        planned.Tuple2(Constant.wrap(new ro.yota.weave.planned.String(n)), p)
    }).toSeq
    Procedure(new UpdateField, Seq(this) ++ updates)
  }

  /// Generate copied version of this plan with checkpointer
  def checkPoint(key: String, fieldPaths: Seq[String] = Seq(), level: Int = CheckPointLevel.Important): Plan[A]

  /**
    * Get plan for stdout of the procedure
    */
  def stdout: Plan[p.TextFile] = {
    root match {
      case root: Plan[Plannable] => {
        new Procedure[p.TextFile](new GetLog, Seq(root, Constant(0)))
      }
      case _ => {
        throw new RuntimeException("Root is not a valid plan")
      }
    }
  }

  /**
    * Get plan for stdout of the procedure
    */
  def stderr: Plan[p.TextFile] = {
    root match {
      case root: Plan[Plannable] => {
        new Procedure[p.TextFile](new GetLog, Seq(root, Constant(1)))
      }
      case _ => {
        throw new RuntimeException("Root is not a valid plan")
      }
    }
  }

}

class FieldAccessPlan[A <: Plannable : TypeTag,
  Parent <: Plannable](val parent: Plan[Parent], val fieldName: String)
    extends Plan[A] {
  val spec = new FieldSpec(parent, fieldName)
  val root = parent.root

  /// Returns path of field names required for accessing to this plan
  /// from the parent launchable plan
  def fieldPath: Seq[String] = {
    var path: Seq[String] = Seq()
    var curplan: Option[FieldAccessPlan[_,_]] = Some(this)

    while (! curplan.isEmpty) {
      path = Seq(curplan.get.fieldName) ++ path
      curplan = curplan.get.parent match {
        case p: FieldAccessPlan[_,_] => Some(p)
        case l: Launchable => None
      }
    }
    path
  }

  def checkPoint(key: String, fieldPath: Seq[String] = Seq(), level: Int = CheckPointLevel.Important): Plan[A]
  = {
    // This doesn't check checkPointingLevel and prefixes, since parent.checkPoint
    // can do that
    new FieldAccessPlan[A, Parent](
      parent.checkPoint(key, Seq(fieldName) ++ fieldPath),
      fieldName)
  }

  def checkPointStatus(publisher: Publisher) =
    root.checkPointStatus(publisher, spec.digest, fieldPath)
}

trait Launchable extends AnyPlan {
  /// Encode necessary information for launching into JsObject
  def launchInfo: JsObject;

  override def launchInfoOpt: Option[JsObject] = Some(launchInfo)

  def root: Launchable = this;

  /// All Launchable can have checkpoints
  /// In contrast, FieldAccessPlan only modifies parent Launchable when a user
  /// requests checkpointing on the field
  def publishingInfo: Seq[PublishingInfo] // path => fieldId

  def checkPointStatus(publisher: Publisher): Seq[CheckPointStatus] = {
    checkPointStatus(publisher, this.spec.digest, Seq())
  }

  def checkPointStatus(publisher: Publisher, expectedDigest: String, targetFieldPath: Seq[String])
      : Seq[CheckPointStatus] = {

    publishingInfo.map { pubinfo =>
      checkPointStatus(publisher, expectedDigest, targetFieldPath, pubinfo)
    }
  }

  def checkPointStatus(
    publisher: Publisher,
    expectedDigest: String, targetFieldPath: Seq[String],
    pubinfo: PublishingInfo): CheckPointStatus = {
    if (pubinfo.fieldNames != targetFieldPath || pubinfo.writeOnly ||
      ! publisher.exists(pubinfo.keyString) ||
      ! publisher.exists(pubinfo.keyString + ".info")) {
      CheckPointNotFound
    } else {
      val infoPath = publisher.retrieve(pubinfo.keyString + ".info").get
      val info = Json.parse(infoPath.string).asInstanceOf[JsObject]

      val launchDigest = (info \ "exportedFrom").as[String]

      if (expectedDigest == launchDigest) {
        CheckPointConsistent(pubinfo.keyString)
      } else {
        CheckPointInconsistent(pubinfo.keyString, launchDigest)
      }
    }
  }

}


