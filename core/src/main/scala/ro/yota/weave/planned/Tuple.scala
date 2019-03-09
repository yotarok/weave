package ro.yota.weave.planned

import ro.yota.weave._

import scala.reflect.runtime.universe._
import scala.language.implicitConversions

class Tuple2[A <: Plannable: TypeTag, B <: Plannable: TypeTag](
  val _1: A,
  val _2: B) extends Container {

  type BaseType = scala.Tuple2[A#BaseType, B#BaseType]

  override def value: scala.Tuple2[A#BaseType, B#BaseType] = scala.Tuple2(
    _1.value.asInstanceOf[A#BaseType],
    _2.value.asInstanceOf[B#BaseType]
  )

  val className =
    s"${getClass.getName}[${typeOf[A].toString},${typeOf[B].toString}]"

  def this(fields: Map[java.lang.String, Plannable]) = {
    this(fields("_1").asInstanceOf[A],
      fields("_2").asInstanceOf[B])
  }


  override def fieldNames: Seq[java.lang.String] = List("_1", "_2")

  override def field(key: java.lang.String) : Plannable = {
    key match {
      case "_1" => _1
      case "_2" => _2
      case s => Plannable.fieldNotFound(s)
    }
  }

  override def updateField(key: java.lang.String, value: Plannable):
      Tuple2[A, B] = {
    new Tuple2[A, B](fieldNames.map({k =>
      if (k == key) {
        k -> value
      } else {
        k -> this.field(k)
      }
    }).toMap)
  }

}

class Tuple2Constructor[A <: Plannable : TypeTag, B <: Plannable : TypeTag]
    extends Function2[Tuple2[A, B], A, B] {
  val className =
    s"${getClass.getName}[${typeOf[A].toString},${typeOf[B].toString}]"
  override val isAuxiliary = true
  def apply(a: A, b: B): Tuple2[A, B] = new Tuple2(a, b)
}

object Tuple2 {
  def apply[A <: Plannable : TypeTag, B <: Plannable : TypeTag]
    (arg1: Plan[A], arg2: Plan[B]): Plan[Tuple2[A, B]]
  = {
    Procedure[Tuple2[A, B]](new Tuple2Constructor[A, B](), Seq(arg1, arg2))
  }
  def apply[A <: Plannable : TypeTag, B <: Plannable : TypeTag](arg1: A, arg2: B): Tuple2[A, B]
  = {
    new Tuple2(arg1, arg2)
  }

  implicit def
    convertToAccessor[A <: Plannable : TypeTag, B <: Plannable : TypeTag]
    (pt: Plan[Tuple2[A, B]]): Tuple2Accessor[A, B] = {
    new Tuple2Accessor(pt)
  }
}

class Tuple2Accessor[A <: Plannable : TypeTag, B <: Plannable : TypeTag](
  val value: Plan[Tuple2[A, B]]) {

  // scalastyle:off method.name
  def _1: Plan[A] = new FieldAccessPlan[A, Tuple2[A, B]](value, "_1")
  def _2: Plan[B] = new FieldAccessPlan[B, Tuple2[A, B]](value, "_2")
  // scalastyle:on method.name
}
