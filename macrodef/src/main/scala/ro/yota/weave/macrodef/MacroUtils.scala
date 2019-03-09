package ro.yota.weave.macrodef

import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions
import scala.annotation.compileTimeOnly

import scala.reflect.macros.blackbox.Context

object MacroUtils {
  def extractAnnotationParameters(c: Context): List[c.universe.Tree] = {
    import c.universe._
    c.prefix.tree match {
      case q"new $name( ..$params )" => params
      case _ => List()
    }
  }

  def extractNamedParameters(c: Context): Map[String, c.universe.Tree] = {
    import c.universe._
    extractAnnotationParameters(c).map { (param =>
      param match {
        case asgn: AssignOrNamedArg => {
          Some(Tuple2(asgn.lhs.toString, asgn.rhs))
        }
        case _ => None
      })
    }.filterNot(_.isEmpty).map(_.get).toMap
  }

  /**
    * Utility function for extracting argument names from argument list
    */
  def extractArgNames(c: Context)(args: Seq[c.universe.ValDef]): List[c.TermName] = {
    import c.universe._
    args.toList.map({case ValDef(flag, argName, argType, defs) =>
      Some(argName)
    }).filterNot(_.isEmpty).map(_.get)
  }

  /**
    * Utility function for extracting val definition from statements
    */
  def extractValDefs(c: Context)(body: Seq[c.universe.Tree]):
      List[(c.universe.TermName, c.universe.Tree)] = {
    import c.universe._
    body.toList.map({ (t:Tree) => t match {
      case q"val $valName: $valType = $rest"  => {
        if (valType.isEmpty) {
          c.abort(c.enclosingPosition,
            "Val fields of weaveData must have explicit type")
        }
        else {
          Some((valName, valType))
        }
      }
      case _ => {
        None
      }
    }}).filterNot(_.isEmpty).map(_.get)
  }


  /**
    * Utility function for ensuring that annottees are a class expression
    */
  def ensureClassExpr(c: Context)(annottees: Seq[c.Expr[Any]])
      : List[c.universe.Tree] = {
    import c.universe._
    val inputs = annottees.map(_.tree).toList
    inputs match {
      case (param: ValDef) :: (rest @ (_ :: _)) =>
        c.abort(c.enclosingPosition, "Expected a class")
      case (param: TypeDef) :: (rest @ (_ :: _)) =>
        c.abort(c.enclosingPosition, "Expected a class")
      case _ => inputs
    }
  }

  /** Return true if macroDebug=true is specified
    */
  def isDebugMode(c: Context): Boolean = {
    import c.universe._
    MacroUtils.extractNamedParameters(c).getOrElse("macroDebug", q"false")
      match {
      case q"true" => true
      case q"false" => false
      case _ =>
        c.abort(c.enclosingPosition,
          "Specify true or false in \"macroDebug\" parameter")
    }
  }

}
