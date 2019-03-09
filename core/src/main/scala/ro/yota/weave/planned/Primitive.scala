package ro.yota.weave.planned

import ro.yota.weave._
import play.api.libs.json._
import play.api.libs.json.Json._

import scalax.io.{Output, Input, Codec}

/** A plannable type that wraps java.lang.String
  *
  * @constructor create a new wrapped object with the given value
  * @param v value (lazy)
  */
class String(v: => java.lang.String) extends Primitive {
  override lazy val value = v
  type BaseType = java.lang.String
  val className = getTypeTag[String](this).tpe.toString

  def this(classLoader: ClassLoader, input: Input) {
    this {
      input.string(Codec.UTF8)
    }
  }

  override def write(output: Output) {
    output.write(value)
  }
}

/** A plannable type that wraps scala.Int
  *
  * @constructor create a new wrapped object with the given value
  * @param v value (lazy)
  */
class Int(v: => scala.Int) extends Primitive {
  override lazy val value = v
  type BaseType = scala.Int
  val className = getTypeTag[Int](this).tpe.toString

  def this(classLoader: ClassLoader, input: Input) {
    this {
      input.string(Codec.UTF8).toInt
    }
  }

  override def write(output: Output) {
    output.write(value.toString)
  }
}

/** A plannable type that wraps scala.Boolean
  *
  * @constructor create a new wrapped object with the given value
  * @param v value (lazy)
  */
class Boolean(v: => scala.Boolean) extends Primitive {
  override lazy val value = v
  type BaseType = scala.Boolean
  val className = getTypeTag[Boolean](this).tpe.toString

  def this(classLoader: ClassLoader, input: Input) {
    this {
      input.string(Codec.UTF8).toBoolean
    }
  }

  override def write(output: Output) {
    output.write(value.toString)
  }
}

/** A plannable type that wraps scala.math.BigDecimal
  *
  * For planning purpose, floating point numbers are not recommended because
  * they sometimes lose exact identity after some computations
  *
  * @constructor create a new wrapped object with the given value
  * @param v value (lazy)
  */
class BigDecimal(v: => scala.math.BigDecimal) extends Primitive {
  override val value = v
  type BaseType = scala.math.BigDecimal
  val className = getTypeTag[BigDecimal](this).tpe.toString


  def this(classLoader: ClassLoader, input: Input) {
    this {
      scala.math.BigDecimal(input.string(Codec.UTF8).toString)
    }
  }

  override def write(output: Output) {
    output.write(value.toString)
  }
}

object BigDecimal {
  def apply(s: java.lang.String): BigDecimal = {
    new BigDecimal(scala.math.BigDecimal(s))
  }
  def apply(d: Double): BigDecimal = {
    new BigDecimal(scala.math.BigDecimal(d))
  }
  def apply(d: scala.math.BigDecimal): BigDecimal = {
    new BigDecimal(d)
  }
}

/** A plannable type that wraps scala.Unit
  *
  * @constructor create a new wrapped object with the given value
  * @param v value (lazy)
  */
class Unit(v: => scala.Unit) extends Primitive {
  override lazy val value = v
  type BaseType = scala.Unit
  val className = getTypeTag[Unit](this).tpe.toString
  def this(classLoader: ClassLoader, input: Input) {
    this(())
  }

  override def write(output: Output) {
    output.write("()")
  }
}

