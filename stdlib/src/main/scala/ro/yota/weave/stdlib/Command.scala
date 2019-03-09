package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.macrodef._

import scala.reflect.runtime.universe._
import sys.process._

@WeaveRecord class GrepOption(
  extended: p.Boolean,
  ignoreCase: p.Boolean,
  invertMatch: p.Boolean) {
  def default = {
    new GrepOption(true, false, false)
  }
  def toArgSeq: Seq[String] = {
    (if (extended.value) {
      Seq("--extended-regexp")
    } else {
      Seq("--basic-regexp")
    }) ++ (if (ignoreCase) {
      Seq("--ignode-case")
    } else {
      Seq()
    }) ++ (if (invertMatch) {
      Seq("--invert-match")
    } else {
      Seq()
    })
  }
}

@WeaveFunction class Grep {
  def apply(opts: GrepOption, pattern: p.String, input: p.TextFile): p.TextFile = {
    val output = p.TextFile.output()
    val cmd = (Seq("grep") ++ opts.value.toArgSeq ++
      Seq(pattern.value, input.path.path))
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}
