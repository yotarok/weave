package ro.yota.weave

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

import ro.yota.weave.macrodef._
import scalax.file.Path

import scalax.io.Input
import scala.language.implicitConversions
import play.api.libs.json._
import play.api.libs.json.Json._
import ro.yota.weave.storage.{Storage, LogType, LogType_Error, LogType_Output}
import ro.yota.weave.{planned => p}
import scalax.io.{Output, Codec}
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

trait Function extends p.Primitive {
  /**
    * Launch the function
    */
  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable;

  /**
    * Return signature of function computed from the give argument digests
    */
  def signature(argsDigest: Seq[String]): String = {
    // Don't use format method a performance bottleneck.
    className + '(' + argsDigest.mkString(",") + ')' + this.version
  }

  /**
    * True if the function doesn't need dedicated session
    */
  def isAuxiliary: Boolean = false

  /**
    * Returns signature for runtime cache computed from runtime
    *   argument digests
    */
  def cacheSignature(argsContentDigest: Seq[String]): String = {
    className + "(" + argsContentDigest.mkString(",") + ")" + this.version
  }

  /**
    * Returns hash value of cacheSignature used as a key for runtime cache
    */
  def cacheKey(inputs: Seq[Plannable]): Option[String] = {
    val contentDigests = inputs.map(_.loadedContentDigest)
    if (! this.enableCache || contentDigests.exists(_.isEmpty)) {
      None
    } else {
      Some(Hasher(this.cacheSignature(contentDigests.map(_.get))))
    }
  }

  def className : String;
  def version : String = "";
  def isConstant: Boolean = false;

  def enableCache: Boolean = ! isConstant

  def prettyPrint(width: Int, argStr: Seq[String]): String = {
    val shortClassName = Prettifier.shortenClassName(className)
    val shortName = (s"${shortClassName}(" +
      argStr.mkString(", "))
    val suffix = if (version.length == 0) {
      ")"
    } else {
      s") [${version}]"
    }
    Prettifier.truncate(shortName, width - suffix.length) + suffix
  }

  private[this] def getFileCompanion[A <: p.File : TypeTag]
      : p.FileCompanionBase[A] = {
    val rootMirror = ru.runtimeMirror(getClass.getClassLoader)
    var classSymbol = ru.typeOf[A].typeSymbol.asInstanceOf[ru.ClassSymbol]
    val moduleMirror = rootMirror.reflectModule(classSymbol.companion.asInstanceOf[ru.ModuleSymbol])
    moduleMirror.instance.asInstanceOf[p.FileCompanionBase[A]]
  }

  def makeOutputFile[A <: p.File : TypeTag](): A = {
    getFileCompanion[A].output()
  }

  def makeTemporaryFile[A <: p.File : TypeTag](): A = {
    getFileCompanion[A].temporary()
  }


  // The followings are for implementing ValuePrimitive trait
  type BaseType = Function

  override def toString: java.lang.String = "Function: " + className

  override def write(output: Output) {
    output.write("")
  }


  def value: BaseType = this
}

class FunctionNotImplemented extends Function {
  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable = ???;
  override def signature(argsDigest: Seq[String]): String =
    "???(" + argsDigest.mkString(",") + ")"

  override def cacheSignature(argsContentDigest: Seq[String]): String = ""

  def className : String = ???;
}


// scalastyle:off magic.number

// Function0 means constant, and this is invoked in planning time
abstract class Function0[Ret <: Plannable : TypeTag] extends Function {
  def apply() : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply()

}

abstract class Function1[Ret <: Plannable : TypeTag, Arg <: Plannable : TypeTag]
    extends Function {
  def apply(a: Arg) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(input(0).asInstanceOf[Arg])
}

abstract class Function2[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2])
}

abstract class Function3[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag,
  Arg3 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2, a3: Arg3) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2],
      input(2).asInstanceOf[Arg3])
}

abstract class Function4[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag,
  Arg3 <: Plannable : TypeTag, Arg4 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2, a3: Arg3, a4: Arg4) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2],
      input(2).asInstanceOf[Arg3],
      input(3).asInstanceOf[Arg4])
}

abstract class Function5[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag,
  Arg3 <: Plannable : TypeTag, Arg4 <: Plannable : TypeTag,
  Arg5 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2, a3: Arg3, a4: Arg4, a5: Arg5) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2],
      input(2).asInstanceOf[Arg3],
      input(3).asInstanceOf[Arg4],
      input(4).asInstanceOf[Arg5])
}

abstract class Function6[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag,
  Arg3 <: Plannable : TypeTag, Arg4 <: Plannable : TypeTag,
  Arg5 <: Plannable : TypeTag, Arg6 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2, a3: Arg3, a4: Arg4, a5: Arg5, a6: Arg6) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2],
      input(2).asInstanceOf[Arg3],
      input(3).asInstanceOf[Arg4],
      input(4).asInstanceOf[Arg5],
      input(5).asInstanceOf[Arg6])
}

abstract class Function7[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag,
  Arg3 <: Plannable : TypeTag, Arg4 <: Plannable : TypeTag,
  Arg5 <: Plannable : TypeTag, Arg6 <: Plannable : TypeTag,
  Arg7 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2, a3: Arg3, a4: Arg4, a5: Arg5, a6: Arg6, a7: Arg7) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2],
      input(2).asInstanceOf[Arg3],
      input(3).asInstanceOf[Arg4],
      input(4).asInstanceOf[Arg5],
      input(5).asInstanceOf[Arg6],
      input(6).asInstanceOf[Arg7])
}

abstract class Function8[Ret <: Plannable : TypeTag,
  Arg1 <: Plannable : TypeTag, Arg2 <: Plannable : TypeTag,
  Arg3 <: Plannable : TypeTag, Arg4 <: Plannable : TypeTag,
  Arg5 <: Plannable : TypeTag, Arg6 <: Plannable : TypeTag,
  Arg7 <: Plannable : TypeTag, Arg8 <: Plannable : TypeTag]
    extends Function {
  def apply(a1: Arg1, a2: Arg2, a3: Arg3, a4: Arg4, a5: Arg5, a6: Arg6, a7: Arg7, a8: Arg8) : Ret;

  def launch(serializer: Serializer, input : Seq[Plannable]) : Plannable =
    this.apply(
      input(0).asInstanceOf[Arg1],
      input(1).asInstanceOf[Arg2],
      input(2).asInstanceOf[Arg3],
      input(3).asInstanceOf[Arg4],
      input(4).asInstanceOf[Arg5],
      input(5).asInstanceOf[Arg6],
      input(6).asInstanceOf[Arg7],
      input(7).asInstanceOf[Arg8])
}

// scalastyle:on magic.number


class Cast[T <: Plannable : TypeTag] extends Function {

  override val isAuxiliary = true

  val className = s"${getClass.getName}[${typeOf[T].toString}]"
  // Current limitation: Since the classes in weave core is tied with
  // AppClassLoader, which doesn't include the specified jars,
  // typeOf[F[T]], where F is a function in the core package fails when it
  // tries to reflect typeTag for T

  def launch(serializer: Serializer, input : Seq[Plannable])
      : Plannable = {
    // The current implementation is not efficient, need refactoring
    val src = input(0)
    if (! src.isPrimitive) {
      throw new RuntimeException("cannot cast record type")
    }

    val newClassName = ru.typeOf[T].toString
    val temppath = Path.createTempFile()
    for {
      processor <- temppath.outputProcessor
    } {
      src.write(processor.asOutput)
    }

    val newInfo = src.basicInfo ++ (
      src.objectKey match {
        case Some(refKey) => {
          Json.obj("className" -> newClassName, "objectRef" -> refKey)
        }
        case None => {
          Json.obj("className" -> newClassName)
        }
      }
    )
    val ret = serializer.loadPrimitiveObject(temppath, newInfo)
    ret.objectKey = src.objectKey
    ret
  }
}

// Update fields directly
class UpdateField
    extends Function {
  override val isAuxiliary = true
  final val className = typeOf[UpdateField].toString

  override def launch(serializer: Serializer, input : Seq[Plannable]):
      Plannable = {
    val updatePairs = input.tail.map {
      case t: planned.Tuple2[_, _] => {
        (t._1, t._2) match {
          case (k: planned.String, v: Plannable) => {
            scala.Tuple2(k.value, v)
          }
          case _ => {
            throw new RuntimeException("Typecheck failed")
          }
        }
      }
      case _ => {
        throw new RuntimeException("One of input is not a tuple")
      }
    }

    var obj = input.head
    val fieldNameSet = obj.fieldNames.toSet
    for ((k, v) <- updatePairs) {
      if (! fieldNameSet.contains(k)) {
        throw new RuntimeException(s"${k} is not a valid field for ${obj}")
      }
      obj = obj.updateField(k, v)
    }
    obj
  }

}

/**
  * Constant function returns the const object specified in the recipe
  */
class Constant[A <: Plannable : TypeTag](
  val x: A,
  val overrideKey: Option[String] = None)
    extends Function0[A] {
  val className = s"${getClass.getName}[${typeOf[A].toString}]"
  // ^ Also see the comments for Cast.className

  lazy val valueSignature: String = {
    overrideKey match {
      case Some(k) => k
      case None => {
        x match {
          case file: p.File => s"FILE[${file.contentDigest}]"
          case dir: p.Directory => s"DIR[${dir.contentDigest}]"
          case _ => {
            x.toString
          }
        }
      }
    }
  }

  override def signature(argsDigest: Seq[String]): String = className + "(" + valueSignature + ")"

  override def isConstant: Boolean = true;
  def apply() : A = x
  override def prettyPrint(width: Int, argStr: Seq[String]): String = {
    Prettifier.truncate(
      x.toString + ": Constant[" +
        Prettifier.shortenClassName(typeOf[A].toString) + "]",
      width)
  }

}
object Constant{
  def apply[A <: Plannable : TypeTag](s: A): Plan[A] =
    wrap(s)
  def wrap[A <: Plannable : TypeTag](s: A): Plan[A] =
    Procedure[A](new Constant[A](s), Seq())
}

class NullFunction[A <: Plannable : TypeTag] extends Function {
  val className = s"${getClass.getName}[${typeOf[A].toString}]"

  def launch(serializer: Serializer, input : Seq[Plannable])
      : Plannable = {
    new planned.Unit(())
  }
}

/**
  * Dummy function for returning computation log of the specified plan
  */
class GetLog[A <: Plannable : TypeTag] extends Function {
  val className = s"${getClass.getName}[${typeOf[A].toString}]"
  // ^ Also see the comments for Cast.className

  override val isAuxiliary = true

  /**
    * Implementation for retrieving and copying log
    */
  def copyLog(serializer: Serializer, key: String, logType: LogType)
      : p.TextFile = {
    val output = p.TextFile.output()

    serializer.storage.loadLog(key, logType) match {
      case Some(path) => {
        path.copyTo(output.path, replaceExisting = true);
        output
      }
      case None => {
        throw new RuntimeException(
          "Log is not found");
      }
    }
  }

  /**
    * Entry point of the function
    */
  override def launch(serializer: Serializer, input: Seq[Plannable]):
      Plannable = {
    val logType = input(1) match {
      case i: p.Int => {
        if (i.value == 0) {
          LogType_Output
        } else if (i.value == 1) {
          LogType_Error
        } else {
          throw new RuntimeException(
            "The second argument must be 0 (stdout) or 1 (stderr)");
        }
      }
      case _ => {
        throw new RuntimeException(
          "The second argument must be 0 (stdout) or 1 (stderr)");
      }
    }

    input.head.objectKey match {
      case Some(key) => {
        copyLog(serializer, key, logType)
      }
      case None => {
        throw new RuntimeException(
          "Key is not in meta data; perhaps, called on a field value?");
      }
    }
  }
}


class AnonymousFunction1[Ret <: Plannable : TypeTag, Arg <: Plannable : TypeTag](
  val description: String,
  val funcTree: scala.reflect.runtime.universe.Tree
) extends Function1[Ret, Arg] {

  private[this] val toolbox = currentMirror.mkToolBox()

  val className =
    s"${getClass.getName}[${typeOf[Ret].toString},${typeOf[Arg].toString}]"
  // See the comments on Cast.className

  override val version = description
  val argTypeStr = typeOf[Arg].toString
  val retTypeStr = typeOf[Ret].toString
  private val augmentedTreeSrc = s"""
val f = ${funcTree.toString};
(arg: ${argTypeStr}) => (f(arg): ${retTypeStr})
"""

  private lazy val augmentedTree = toolbox.parse(augmentedTreeSrc)

  lazy val funcImpl = toolbox.compile(augmentedTree)().asInstanceOf[(Arg) => Ret]

  def this(classLoader: ClassLoader, input: Input) {
    this ("DummyValue [Only needed in plan-time]", {
      val toolbox = currentMirror.mkToolBox()
      toolbox.parse(input.string(Codec.UTF8))
    })
  }

  override def write(output: Output) {
    output.write(funcTree.toString)
  }

  // Constant uses string representation for constructing signature
  override def toString: String = {
    s"LambdaFunction[${description}]"
  }

  def apply(a: Arg): Ret = {
    funcImpl(a);
  }
}
