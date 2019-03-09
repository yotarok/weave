package ro.yota.weave.macrodef

import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions

import scala.reflect.macros.blackbox.Context

class WeaveRecord extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro WeaveRecord.impl
}

object WeaveRecord {
  /**
    * Write inside class for giving transparent access to fields via
    *   implicit conversion
    *
    * @param parentClassName Class name of record
    * @param className Class name for internal constructor function class
    *     (typically "Accessor")
    * @param accessibleFields List of tuple of accessible field names and
    *     field types
    */
  private
  def writeAccessorClass(c: Context)(
    parentClassName: c.universe.TypeName,
    className: c.universe.TypeName,
    accessibleFields: Seq[(c.universe.TermName, c.universe.Tree)]
  ): c.universe.ClassDef = {
    import c.universe._

    val accFields = accessibleFields.map( {case (valName, valType) =>
      val fieldStr = Literal(Constant(valName.toString));
      q"def $valName: Plan[$valType] = new FieldAccessPlan[$valType, $parentClassName](data, $fieldStr);"
    } )

    q"""
class $className(val data: Plan[$parentClassName]) {
  ..$accFields;
}
"""
  }

  /**
    * Write inside class for calling constructor of record
    *
    * @param parentClassName Class name of record
    * @param className Class name for internal constructor function class
    *     (typically "New")
    * @param ctorArgTypes Sequence of argument types
    */
  private
  def writeConstructorClass(c: Context)(
    parentClassName: c.universe.TypeName,
    className: c.universe.TypeName,
    ctorArgTypes: Seq[c.universe.Tree]
  ): c.universe.ClassDef = {
    import c.universe._
    val ctorParentType = TypeName("Function" + ctorArgTypes.length)
    val ctorApplyArgs = ctorArgTypes.zipWithIndex map {
      case (typ, i) =>
        ValDef(Modifiers(Flag.PARAM), TermName("arg" + i), typ, EmptyTree)
    }
    val ctorApplyArgRefs = ctorApplyArgs.zipWithIndex map {
      case (_, i) => Ident(TermName("arg" + i))
    }
    q"""
class $className
     extends $ctorParentType[$parentClassName, ..$ctorArgTypes] {
   final val className = scala.reflect.runtime.universe.typeTag[$className].tpe.toString
   def apply (..$ctorApplyArgs): $parentClassName =
     new $parentClassName(..$ctorApplyArgRefs)
}
"""
  }

  /**
    * Write inside class for constructing record with a default paramter
    */
  private
  def writeDefaultConstructorClass(c: Context)(
    parentClassName: c.universe.TypeName
  ): c.universe.ClassDef = {
    import c.universe._
    val parentClassTerm = TermName(parentClassName.decodedName.toString)
    q"""
class Default extends Function0[$parentClassName] {
   final val className = scala.reflect.runtime.universe.typeTag[Default].tpe.toString
   override def isAuxiliary = true;
   def apply(): $parentClassName = $parentClassTerm.default 
}
"""
  }

  /**
    * Write companion object for providing the following features
    *  - Defining 3 internal classes (New, Default, Accessor)
    *  - Generate construction function (New) the record via "apply" method
    *  - Generate default constructor (Default) via "apply" with no argument
    *  - Implicitly convert Plan[A] to A.Accessor for transparent field access
    */
  private
  def writeCompanionObject(c: Context)(
    className: c.universe.TypeName,
    ctorValDefs: Seq[(c.universe.TermName, c.universe.Tree)],
    fieldValDefs: Seq[(c.universe.TermName, c.universe.Tree)],
    defaultMethod: Option[c.universe.Tree]):
      c.universe.ModuleDef = {
    import c.universe._

    val (ctorArgNames, ctorArgTypes) = ctorValDefs.unzip

    // Companion object has the following 2 internal procedure classes
    val classTerm = TermName(className.decodedName.toString)
    val ctorFuncType = TypeName("New")
    val accType = TypeName("Accessor")

    // Constructor arguments wrapped with Plan[.]
    val ctorPlanArgs = ctorArgTypes.zipWithIndex map { case (typ, i) =>
      ValDef(Modifiers(Flag.PARAM), TermName("arg" + i),
        AppliedTypeTree(Ident(TypeName("Plan")), List(typ)), EmptyTree)
    }

    val ctorPlanArgRefs = ctorArgTypes.zipWithIndex map {
      case (_, i) => Ident(TermName("arg" + i))
    }

    // "apply" function that calls newFunc
    val ctorWrap = q"def apply(..$ctorPlanArgs): Plan[$className] = { Procedure(new $ctorFuncType (), Seq(..$ctorPlanArgRefs)) }"

    // implicit converter
    val impConv = q"implicit def convertToAccessor(data: Plan[$className]): $accType = new $accType(data)"

    val ctorExpr = writeConstructorClass(c)(
      className, ctorFuncType, ctorArgTypes)

    val accExpr = writeAccessorClass(c)(
      className, accType, ctorValDefs ++ fieldValDefs)


    val defctorexprs = if (! defaultMethod.isEmpty) {
      List(defaultMethod.get,
        writeDefaultConstructorClass(c)(className),
        q"def apply(): Plan[$className] = { Procedure(new Default(), Seq()) }")
    } else { List() }

    val objbody = List(ctorWrap, accExpr, impConv, ctorExpr) ++ defctorexprs

    q"object $classTerm { ..$objbody }"
  }

  /**
    * Implementation of recordMacro
    */
  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = { // scalastyle:ignore
    import c.universe._
    val inputs = annottees.map(_.tree).toList

    val classexpr = MacroUtils.ensureClassExpr(c)(annottees)

    val (nclassexpr, objexpr) = classexpr match {
      case (classdef @ q"class $className(..$ctorArgs) extends ..$parents{..$body}") :: Nil => {
        val classTerm = TermName(className.decodedName.toString)

        // Extract constructor info
        val ctorArgTypes = ctorArgs.map {arg => arg.children.head}
        val ctorArgNames = MacroUtils.extractArgNames(c)(ctorArgs)

        // Extract field info
        val ctorValDefs = ctorArgNames zip ctorArgTypes
        val fieldValDefs = MacroUtils.extractValDefs(c)(body)

        // Extract default method
        val defaultMethod = body.map({ (t:c.universe.Tree) => t match {
          case q"def default = $defBody"  =>  Some(t)
          case _ => None
        }}).filterNot(_.isEmpty).map(_.get).lastOption


        val objexpr = writeCompanionObject(c)(className, ctorValDefs, fieldValDefs, defaultMethod)

        val fieldCases =
          (ctorValDefs ++ fieldValDefs).map( {case (valName, valType) =>
            val fieldStr = Literal(Constant(valName.toString));
            cq"$fieldStr => $valName"
            //CaseDef(fieldStr, q"$valName")
          })
        val fieldCaseMatcher = Match(q"key", fieldCases)

        val fieldNames =
          (ctorValDefs ++ fieldValDefs).map( {case (valName, valType) =>
            Literal(Constant(valName.toString));
          })

        val nparents = if (parents.head.toString == "scala.AnyRef") {
          // Overwrite parent class
          val otraits = parents.filter { _.toString != "scala.AnyRef" }
          val clsLocation = c.mirror.staticClass("ro.yota.weave.planned.Container")
          val parentTypeName = Ident(clsLocation)
          List(parentTypeName) ++ otraits
        } else {
          parents
        }


        val classNameStr = classTerm.toString
        val classprefix = q"""
type BaseType = $className ;
def className = scala.reflect.runtime.universe.typeTag[$className].tpe.toString ;
override def field(key: java.lang.String) : Plannable = { $fieldCaseMatcher } ;
override def fieldNames = Seq(..$fieldNames) ;
def value = this;
override def updateField(key: java.lang.String, value: Plannable) = {
  new $className(fieldNames.map({k =>
  if (k == key) {
    k -> value
  } else {
    k -> this.field(k)
  }
}).toMap)
}
""".children

        val initTerm = TermName("<init>")
        val auxCtorParams = ctorValDefs.map( { case (valName, valType) =>
          val ckey = Literal(Constant(valName.toString));
          q"fields($ckey).asInstanceOf[$valType]"
        })
        // So hacky... see the following URL for details.
        val defaultCtorPos = c.enclosingPosition
        val newCtorPos = defaultCtorPos.withEnd(defaultCtorPos.end + 1).
          withStart(defaultCtorPos.start + 1).withPoint(defaultCtorPos.point + 1)
        val classappendix = atPos(newCtorPos)(q"""
        def $initTerm(fields: Map[java.lang.String, Plannable]) =
        $initTerm(..${auxCtorParams});
        """)

        val nclassdef = q"class $className(..$ctorArgs) extends ..$nparents { ..$classprefix ; ..$body  ; ..$classappendix }"

        (nclassdef, objexpr)

      }
      case _ => {
        c.abort(c.enclosingPosition, "Expected a class without companion object")
      }

    }
    val output = Nil :+ nclassexpr :+ objexpr
    if (MacroUtils.isDebugMode(c)) {
      println("==== Weave record definition rewritten ====")
      println(output)
    }
    c.Expr[Any](Block(output, Literal(c.universe.Constant(()))))
  }
}
