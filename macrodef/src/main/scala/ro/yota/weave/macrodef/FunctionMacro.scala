package ro.yota.weave.macrodef

//import scala.reflect.macros.Context
import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions
import scala.annotation.compileTimeOnly

//import scala.reflect.macros._
import scala.reflect.macros.blackbox.Context

@compileTimeOnly("enable macro paradise to expand macro annotations")
class WeaveFunction extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro WeaveFunction.impl
}

/**
  * Generate function class
  *
  * This macro does:
  *  - Generate companion object that has
  *    - apply method for creating Procedure[Ret] instance
  *  - Add context bounds TypeTag for all the type parameters
  *  - Add the parent class "FunctionN" depending on the parameter and return types
  *    of apply method
  *  - Add an alternative constructor for supporting weave serialization
  */
object WeaveFunction {

  /**
    * Find an apply method from the class definition, and returns expressions
    *  for return type and argument types
    */
  private[this]
  def findArgTypeListsFromApplyMethod(c: Context)(body: Seq[c.universe.Tree]):
      (c.universe.Tree, List[c.universe.Tree]) = {
    import c.universe._

    val info = body.map {
      case q"def apply(..$paramlist): $retexpr = $body" =>
        Some(retexpr, paramlist)
      case _ => None
    }.filterNot(_.isEmpty).head match {
      case Some((retexpr, paramlist)) => {
        if (retexpr.isEmpty) {
          c.abort(c.enclosingPosition,
            "The apply method must have explicit return type")
        }
        val argTypes = paramlist map {arg =>
          //println(s"arg.isDef = ${arg.isDef}")
          arg.children.head
        }
        (retexpr, argTypes.toList)
      }
      case _ =>
        c.abort(c.enclosingPosition, "Expected to have an apply method")
    }
    info
  }

  /**
    * Rewrite base class to the default depending on the number of arguments
    *
    * This feature is just a syntax sugar for function writers
    */
  private[this]
  def rewriteBaseClassExpr(c: Context)(parents: Seq[c.universe.Tree],
    className: c.universe.Tree,
    retType: c.universe.Tree,
    argTypes: Seq[c.universe.Tree]):
      Seq[c.universe.Tree] = {
    import c.universe._

    if (parents.head.toString == "scala.AnyRef") {
      // Overwrite parent class
      val otraits = parents.filter { _.toString != "scala.AnyRef" }
      val parentTypeName =
        Ident(TypeName("Function" + argTypes.length))
      val parentType =
        AppliedTypeTree(parentTypeName, List(retType)
          ++ argTypes)
      List(parentType) ++ otraits
    } else {
      parents
    }
  }

  /**
    * Main rewriter function for FunctionMacro
    */
  def rewriteClassDef(c: Context)(
    className: c.universe.TypeName, parents: List[c.Tree],
    body: Seq[c.Tree], typeArgsOpt: Option[Seq[c.universe.TypeDef]])
      : (List[c.Tree], c.universe.ModuleDef) = {
    import c.universe._

    val (retType, argTypes) = findArgTypeListsFromApplyMethod(c)(body)
    val objectName = TermName(className.decodedName.toString)

    val retPlanType = AppliedTypeTree(Ident(TypeName("Plan")),
      List(retType))

    val typeArgNamesOpt = typeArgsOpt.map(_.map(_.name))

    val baseClass = typeArgNamesOpt
      .map(a => tq"$className[..$a]").getOrElse(Ident(className))

    val nparents =
      rewriteBaseClassExpr(c)(parents, baseClass, retType, argTypes)

    val implicitDefs = typeArgsOpt match {
      case Some(typeArgs) => typeArgs.zipWithIndex.map({ case (arg, idx) =>
        val name = arg.name
        val evidenceName = TermName(s"evidence_${idx}")
        q"implicit private[this] val $evidenceName: scala.reflect.runtime.universe.TypeTag [$name];"
      })
      case None => Seq()
    }

    val defaultCtorPos = c.enclosingPosition
    val altCtorPos = defaultCtorPos.withEnd(defaultCtorPos.end + 1).
          withStart(defaultCtorPos.start + 1).withPoint(defaultCtorPos.point + 1)
    val altConstructors = atPos(altCtorPos)(if (implicitDefs.size == 0) {
      q"""def this(classLoader: ClassLoader, input: scalax.io.Input) = this()"""
    } else {
      q"""def this(classLoader: ClassLoader, input: scalax.io.Input)(..$implicitDefs) = this()"""
    })

    val classNameDef = q"""final val className = scala.reflect.runtime.universe.typeTag[$baseClass].tpe.toString;"""

    val nclassdef = typeArgsOpt match {
      case None => q"class $className extends ..$nparents { $altConstructors;  $classNameDef; ..$body }"
      case Some(typeArgs) => q"""class $className[..$typeArgs](..$implicitDefs)
        extends ..$nparents { $altConstructors; $classNameDef;  ..$body }"""
    }

    val argPlanTypes = argTypes.zipWithIndex map {
      case (typ, i) =>
        ValDef(Modifiers(Flag.PARAM), TermName("arg" + i),
          AppliedTypeTree(Ident(TypeName("Plan")),
            List(typ)), EmptyTree)
    }
    val narg = argTypes.length

    val procType = AppliedTypeTree(Ident(TypeName("Procedure")),
      List(retType))
    val argRefs = argTypes.zipWithIndex map {
      case (_, i) => Ident(TermName("arg" + i))
    }

    val objexpr = typeArgsOpt match {
      case None => q"""object $objectName {
  def apply( ..$argPlanTypes ) = ro.yota.weave.Procedure[$retType](new $className(), Seq(..${argRefs}));
}"""
      case Some(typeArgs) => {
        val typeArgNames = typeArgNamesOpt.get
        q"""object $objectName {
  def apply[..$typeArgs]( ..$argPlanTypes )( ..$implicitDefs ) =
    ro.yota.weave.Procedure[$retType](new $className[..$typeArgNames](), Seq(..${argRefs}));
}"""
      }
    }
    ((nclassdef :: Nil), objexpr)
  }

  /**
    * Implementation of FunctionMacro
    */
  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val classexpr = MacroUtils.ensureClassExpr(c)(annottees)
    val (nclassexpr, objexpr) = classexpr match {
      case q"class $className extends ..$parents { ..$body }" :: Nil => {
        rewriteClassDef(c)(className, parents, body, None)
      }
      case (classdef @ q"class $className[..$typeArgs] extends ..$parents { ..$body }") :: Nil => {
        rewriteClassDef(c)(className, parents, body, Some(typeArgs))
      }
      case failed => {
        c.abort(c.enclosingPosition,
          "WeaveFunction annotation is given to an invalid class:\n"
            + failed.toString + """
Please make sure that the class doesn't meet the following conditions:
- Has a context bound for type parameters (TypeTag bounds will be automatically added)
- Has a companion object definition
""")
      }
    }
    val output = nclassexpr :+ objexpr
    if (MacroUtils.isDebugMode(c)) {
      println("==== Weave function definition rewritten ====")
      println(output)
    }

    c.Expr[Any](Block(output, Literal(c.universe.Constant(()))))
  }
}
