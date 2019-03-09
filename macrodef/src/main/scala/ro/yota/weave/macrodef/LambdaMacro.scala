package ro.yota.weave.macrodef

import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions
import scala.annotation.compileTimeOnly
import scala.reflect.runtime.{universe => ru}

import scala.reflect.macros.blackbox.Context

case class LambdaData(val desc: String, val tree: scala.reflect.runtime.universe.Tree)

object Lambda {
  def lambda1Impl(c: Context)(desc: c.Tree, body: c.Tree): c.Tree = {
    import c.universe._
    val ret = q"""{
val tree: scala.reflect.runtime.universe.Tree = scala.reflect.runtime.universe.reify(${body}).tree;
LambdaData(${desc}, tree)
}
"""
    ret
  }
  def lambda(desc: String, body: (_ => _)): LambdaData = macro Lambda.lambda1Impl
}
