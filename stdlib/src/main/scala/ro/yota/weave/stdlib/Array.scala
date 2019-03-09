package ro.yota.weave.stdlib

import ro.yota.weave._
import ro.yota.weave.storage._

import scala.reflect.runtime.universe._
import scalax.io.{Output, Input, Codec}
import scala.language.implicitConversions
import ro.yota.weave.{planned => p}

class WeaveArray[A <: Plannable](
  v: => IndexedSeq[A]) extends WeaveSeq[A] {

  override lazy val value = v
  val className =
    "ro.yota.weave.stdlib.WeaveArray"

  def this(fields: Map[java.lang.String, Plannable]) = {
    this(
      ((0 to (fields("length").asInstanceOf[p.Int].value-1)).map { i =>
        fields(i.toString).asInstanceOf[A]
      }.toIndexedSeq)
    )
  }

  override def fieldNames = {
    (0 to (value.length-1)).map(_.toString)
  } ++ Seq("length")

  override def field(key: java.lang.String) : Plannable = {
    key match {
      case "length" => new p.Int(value.length)
      case _ => try {
        val idx = key.toInt
        if (idx >= value.length) {
          Plannable.fieldNotFound(key)
        } else {
          value(idx)
        }
      } catch {
        case e: java.lang.NumberFormatException =>
          Plannable.fieldNotFound(key)
      }
    }
  }


  override def updateField(key: java.lang.String, value: Plannable) = {
    new WeaveArray[A](fieldNames.map({k =>
      if (k == key) {
        k -> value
      } else {
        k -> this.field(k)
      }
    }).toMap)
  }

}

class WeaveArrayConstructor[A <: Plannable : TypeTag] extends ro.yota.weave.Function {
  final val className = typeTag[WeaveArrayConstructor[A]].tpe.toString

  override def isAuxiliary = true
  def launch(serializer: Serializer, input: Seq[Plannable])
      : Plannable = {
    new WeaveArray(input.map { obj => obj.asInstanceOf[A] }.toIndexedSeq)
  }
}


object WeaveArray {
  def apply[A <: Plannable : TypeTag](elems: Plan[A]*) =
    Procedure[WeaveArray[A]](new WeaveArrayConstructor[A](), elems)

  def apply[A <: Plannable : TypeTag](elems: A*) =
    new WeaveArray(elems.toIndexedSeq)

  def empty[A <: Plannable : TypeTag] =
    new WeaveArray(IndexedSeq[A]())

  implicit def fromSeq[A <: Plannable : TypeTag](elems: Seq[A]) =
    WeaveArray(elems:_*)

  implicit def fromSeq[A <: Plannable : TypeTag](elems: Seq[Plan[A]]) =
    Procedure[WeaveArray[A]](new WeaveArrayConstructor[A](), elems)

  implicit def convertToAccessor
    [A <: Plannable : TypeTag]
    (pt: Plan[WeaveArray[A]]) = 
    new WeaveArrayAccessor(pt)
}


class WeaveArrayAccessor[A <: Plannable : TypeTag](
  val value: Plan[WeaveArray[A]]) {

  def apply(idx: p.Int) = {
    new FieldAccessPlan[A, WeaveArray[A]](value, idx.toString)
  }
  def length = new FieldAccessPlan[A, WeaveArray[A]](value, "length")

  def unlift(size: scala.Int): IndexedSeq[Plan[A]] = {
    (0 until size).map({
      i => this.apply(new p.Int(i))
    }).toIndexedSeq
  }
}


