package ro.yota.weave.stdlib

import ro.yota.weave._
import scala.reflect.runtime.universe._
import ro.yota.weave.{planned => p}
import scala.language.implicitConversions

abstract class WeaveOption[A <: Plannable]() extends p.Container {
  override type BaseType = scala.Option[A#BaseType]

  def map[B <: Plannable](f: (A) => B): WeaveOption[B] = {
    if (this.isEmpty) {
      new WeaveNoneT()
    } else {
      new WeaveSome(f(this.get))
    }
  }
  def mapValue[B](f: (BaseType) => B): scala.Option[B] = {
    if (this.isEmpty) {
      scala.None
    } else {
      scala.Some(f(this.value))
    }
  }

  def get: A;
  def isEmpty : Boolean;

  def getOrElse(default: => A): A = {
    if (this.isEmpty) { default } else { this.get }
  }
}

class WeaveSome[A <: Plannable](val pvalue: A) extends WeaveOption[A] {
  override def value = scala.Some(pvalue.value)

  def get = pvalue;

  val className =
    "ro.yota.weave.stdlib.WeaveSome"

  def this(fields: Map[java.lang.String, Plannable]) =
    this(fields("value").asInstanceOf[A])

  override def fieldNames = List("value")

  def isEmpty = false

  override def field(key: java.lang.String) : Plannable = {
    key match {
      case "value" => this.get
      case s => Plannable.fieldNotFound(s)
    }
  }

  override def updateField(key: java.lang.String, value: Plannable) = {
    new WeaveSome[A](fieldNames.map({k =>
      if (k == key) {
        k -> value
      } else {
        k -> this.field(k)
      }
    }).toMap)
  }

}

class WeaveSomeConstructor[A <: Plannable : TypeTag]
    extends Function1[WeaveSome[A], A] {
  final val className = scala.reflect.runtime.universe.typeTag[WeaveSomeConstructor[A]].tpe.toString
  override def apply(v: A) : WeaveSome[A] = new WeaveSome[A](v)
}

object WeaveSome {
  def apply[A <: Plannable : TypeTag](v: Plan[A]) =
    Procedure[WeaveSome[A]](new WeaveSomeConstructor[A](), Seq(v))
  def apply[A <: Plannable : TypeTag](v: A) = new WeaveSome(v)
}

// Unlike native scala, none has to be a class
class WeaveNoneT[A <: Plannable]() extends WeaveOption[A] {
  //override type BaseType = scala.None
  override def value = scala.None

  val className =
    "ro.yota.weave.stdlib.WeaveNoneT"

  def this(fields: Map[java.lang.String, Plannable]) =
    this()

  def isEmpty = true

  def get = throw new RuntimeException("Do not call get of WeaveNoneT")

  override def fieldNames = List()

  override def field(key: java.lang.String) : Plannable = {
    key match {
      case s => Plannable.fieldNotFound(s)
    }
  }

  override def updateField(key: java.lang.String, value: Plannable) = {
    throw new RuntimeException("Do not update a field of WeaveNoneT")
  }

}

class WeaveNoneConstructor[A <: Plannable : TypeTag]
    extends Function0[WeaveNoneT[A]] {
  final val className = scala.reflect.runtime.universe.typeTag[WeaveNoneConstructor[A]].tpe.toString
  override def apply() : WeaveNoneT[A] = new WeaveNoneT[A]()
}

case object WeaveNone {
  def makeNonePlan[A <: Plannable : TypeTag] =
    Procedure[WeaveNoneT[A]](new WeaveNoneConstructor[A](), Seq())

  implicit def makeNonePlan[A <: Plannable : TypeTag](v: WeaveNone.type):
      Plan[WeaveOption[A]] =
    WeaveNone.makeNonePlan[A]

  implicit def convertNone[B <: Plannable : TypeTag](n: scala.None.type)
      : WeaveNoneT[B] = {
    new WeaveNoneT[B]()
  }
}
