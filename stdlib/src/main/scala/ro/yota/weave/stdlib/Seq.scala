package ro.yota.weave.stdlib

import ro.yota.weave._
import ro.yota.weave.storage._

import scala.reflect.runtime.universe._
import scalax.io.{Output, Input, Codec}
import scala.language.implicitConversions
import ro.yota.weave.{planned => p}

trait WeaveSeq[A <: Plannable] extends p.Container {
  override type BaseType = IndexedSeq[A]

  val value: IndexedSeq[A];
}

object WeaveSeq {
  /* Duplicated definition here is prepared just as syntax sugar
   *  (copied from Array.scala)
   */
  def apply[A <: Plannable : TypeTag](elems: Plan[A]*): Plan[WeaveArray[A]] =
    Procedure[WeaveArray[A]](new WeaveArrayConstructor[A](), elems)

  def apply[A <: Plannable : TypeTag](elems: A*): WeaveArray[A] =
    new WeaveArray(elems.toIndexedSeq)

  def empty[A <: Plannable : TypeTag]: WeaveArray[A] =
    new WeaveArray(IndexedSeq[A]())

  implicit def fromSeq[A <: Plannable : TypeTag](elems: Seq[A])
      : WeaveArray[A] =
    WeaveArray(elems:_*)

  implicit def fromSeq[A <: Plannable : TypeTag](elems: Seq[Plan[A]])
      : Plan[WeaveArray[A]] =
    Procedure[WeaveArray[A]](new WeaveArrayConstructor[A](), elems)

}

