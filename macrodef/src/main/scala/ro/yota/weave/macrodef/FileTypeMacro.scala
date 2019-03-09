package ro.yota.weave.macrodef

import scala.reflect.macros.blackbox.Context
import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions
import scala.annotation.compileTimeOnly

//import scala.reflect.macros._

@compileTimeOnly("enable macro paradise to expand macro annotations")
class WeaveFileType extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro WeaveFileType.impl
}

// Generate utility companion object, and add className
object WeaveFileType {
  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val classexpr = MacroUtils.ensureClassExpr(c)(annottees)
    val (nclassexpr, objexpr) = classexpr match {
      case (classdef @ q"class $className(..$ctorArgs) extends ..$parents { ..$body } ")::Nil => {
        val objectName = TermName(className.decoded)

        val nclassdef = q"""
class $className(..$ctorArgs) extends ..$parents {
  ..$body;
  override val className = getTypeTag[$className](this).tpe.toString;
}
"""
        val objexpr = q"""
object $objectName extends ro.yota.weave.planned.FileCompanionBase[$className] {
  def temporary(): $className = new $className(Path.createTempFile()) ;
}
"""
        ((nclassdef :: Nil), objexpr)
      }
      case failed => {
        c.abort(c.enclosingPosition,
          "Expected a class without companion object:\n" + failed.toString)
      }
    }
    val output = nclassexpr :+ objexpr
    if (MacroUtils.isDebugMode(c)) {
      println("==== Weave file-type definition rewritten ====")
      println(output)
    }

    c.Expr[Any](Block(output, Literal(c.universe.Constant(()))))

  }

}

