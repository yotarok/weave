package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._

import ro.yota.weave.macrodef._

import sys.process._
import scala.util.matching.Regex

@WeaveFunction
class Split {
  def apply(
    s: p.String, sep: p.String): WeaveArray[p.String] = {
    WeaveArray(s.value.split(sep.value).map(new p.String(_)):_*)
  }
}

@WeaveFunction
class JoinString {
  def apply(
    texts: WeaveSeq[p.String], separator: p.String) : p.String = {
    texts.value.mkString(separator.value)
  }
}

@WeaveFunction
class ToLowerCase {
  def apply(s: p.String): p.String = {
    s.value.toLowerCase()
  }
}

@WeaveFunction
class ToUpperCase {
  def apply(s: p.String): p.String = {
    s.value.toUpperCase()
  }
}

/**
  * Write string to a file
  *
  * In general, p.String can be directly casted to TextFile.
  * This function is mainly for debugging purpose
  */
@WeaveFunction
class WriteToFile {
  def apply(s: p.String): p.TextFile = {
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      out.write(s.value)
    }
    output
  }
}
