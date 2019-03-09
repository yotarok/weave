package ro.yota

import ro.yota.weave.macrodef.LambdaData

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import ro.yota.weave.{planned => p}
import scalax.file.{Path}

package object weave {
  implicit def convertFromBoolean(x : Boolean) : Plan[p.Boolean] =
    Constant.wrap[p.Boolean](x)
  implicit def convertFromString(x : String) : Plan[p.String] =
    Constant.wrap[p.String](x)
  implicit def convertFromInt(x : Int) : Plan[p.Int] =
    Constant.wrap[p.Int](x)
  implicit def convertFromBigDecimal(x: scala.math.BigDecimal): Plan[p.BigDecimal] =
    Constant.wrap[p.BigDecimal](x)


  implicit def liftTuple2[A, B, PA <: Plannable : TypeTag, PB <: Plannable : TypeTag]
    (t: Tuple2[A, B])(implicit fA: A => PA, fB: B => PB)
      : p.Tuple2[PA, PB] = {
    p.Tuple2(fA(t._1), fB(t._2))
  }
  implicit def liftTuple2[A, PA <: Plannable : TypeTag, PB <: Plannable : TypeTag]
    (t: Tuple2[A, PB])(implicit fA: A => PA)
      : p.Tuple2[PA, PB] = {
    p.Tuple2(fA(t._1), t._2)
  }
  implicit def liftTuple2[A, PA <: Plannable : TypeTag, PB <: Plannable : TypeTag]
    (t: Tuple2[A, Plan[PB]])(implicit fA: A => PA)
      : Plan[p.Tuple2[PA, PB]] = {
    p.Tuple2(Constant.wrap[PA](fA(t._1)), t._2)
  }
  implicit def liftTuplePlan2[A, B, PA <: Plannable : TypeTag, PB <: Plannable : TypeTag]
    (t: Tuple2[A, B])(implicit fA: A => PA, fB: B => PB)
      : Plan[p.Tuple2[PA, PB]] = {
    p.Tuple2(Constant.wrap[PA](fA(t._1)), Constant.wrap[PB](fB(t._2)))
  }

  implicit def planBoolean(x : Boolean) : p.Boolean = new p.Boolean(x)
  implicit def planString(x : String) : p.String = new p.String(x)
  implicit def planInt(x : Int) : p.Int = new p.Int(x)
  implicit def planBigDecimal(x : scala.math.BigDecimal): p.BigDecimal =
    new p.BigDecimal(x)
  implicit def planTuple2[T1 <: Plannable : TypeTag, T2 <: Plannable : TypeTag]
    (t : Tuple2[T1, T2])
      : p.Tuple2[T1, T2] =
    new p.Tuple2(t._1, t._2)

  /** Inject a file from the specified path */
  def injectFile(x : String): Plan[p.File] = {
    injectFile(Path.fromString(x))
  }

  /** Inject a file from the specified path */
  def injectFile(x : Path): Plan[p.File] = {
    Constant[p.File](new p.File(x))
  }

  /** Inject a directory from the specified path */
  def injectDirectory(x : String): Plan[p.Directory] = {
    injectDirectory(Path.fromString(x))
  }

  /** Inject a directory from the specified path */
  def injectDirectory(x : Path): Plan[p.Directory] = {
    Constant[p.Directory](new p.Directory(x))
  }


  /** Inject a file with the given predefined digest from the specified path */
  def injectFileWithKey(x : String, key: String): Plan[p.File] = {
    injectFileWithKey(Path.fromString(x), key)
  }

  /** Inject a file with the given predefined digest from the specified path */
  def injectFileWithKey(x : Path, key: String): Plan[p.File] = {
    Procedure[p.File](new Constant[p.File](new p.File(x), Some(key)), Seq())
  }

  /** Inject a directory with the given predefined digest from the specified path */
  def injectDirectoryWithKey(x : String, key: String): Plan[p.Directory] = {
    injectDirectoryWithKey(Path.fromString(x), key)
  }

  /** Inject a directory with the given predefined digest from the specified path */
  def injectDirectoryWithKey(x : Path, key: String): Plan[p.Directory] = {
    Procedure[p.Directory](new Constant[p.Directory](new p.Directory(x), Some(key)), Seq())
  }


  implicit def deplanBoolean(x: p.Boolean): Boolean = x.value
  implicit def deplanString(x: p.String): String = x.value
  implicit def deplanInt(x: p.Int): Int = x.value
  implicit def deplanBigDecimal(x : p.BigDecimal): scala.math.BigDecimal = x.value
  implicit def deplanTuple2[T1 <: Plannable : TypeTag, T2 <: Plannable : TypeTag]
    (x: p.Tuple2[T1, T2]): Tuple2[T1#BaseType, T2#BaseType] = x.value

  implicit def lambdaToFunction1
    [R <: Plannable : TypeTag, A0 <: Plannable : TypeTag](lm: LambdaData):
      Function1[R, A0] = new AnonymousFunction1[R, A0](lm.desc, lm.tree)
  implicit def lambdaToFunction1plan
    [R <: Plannable : TypeTag, A0 <: Plannable : TypeTag](lm: LambdaData):
      Plan[Function1[R, A0]] = Constant.wrap[Function1[R, A0]](
    new AnonymousFunction1[R, A0](lm.desc, lm.tree))
}
