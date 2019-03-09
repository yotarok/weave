package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import scala.language.implicitConversions

import ro.yota.weave.macrodef._
import scalax.file.Path

import sys.process._
import scala.util.matching.Regex

import play.api.libs.json.{Json, JsValue}

@WeaveFileType class JsonFile(p: Path) extends p.TextFile(p) {
  def parse(): JsValue = Json.parse(p.string)
}

// In Weave stdlib, even though jq query language is used,
// the implementaiton (jq) should be encapsulated, i.e. client must be agnostic
// to the jq options.

/**
  * Get text file from Json strings
  */
@WeaveFunction class GetTextFromJson {
  def apply(input: JsonFile): p.TextFile = {
    val output = p.TextFile.output()
    //val cmd: ProcessBuilder = Seq("jq", "--raw-output")
    val cmd: ProcessBuilder = Seq("jq", "-r", ".")
    ProcessUtils.execute(cmd #< input.asFile #> output.asFile)
    output
  }
}

/**
  * Select Json elements by the jq query language
  */
@WeaveFunction class QueryJson {
  def apply(query: p.String, input: JsonFile): JsonFile = {
    val output = JsonFile.output()
    val cmd: ProcessBuilder = Seq("jq", query.value)
    ProcessUtils.execute(cmd #< input.asFile #> output.asFile)
    output
  }
}

/**
  * Join several JSON files and makes an array
  */
@WeaveFunction class MakeJsonArray {
  def apply(inputs: WeaveSeq[JsonFile]): JsonFile = {
    val output = JsonFile.output()
    val cmd1: ProcessBuilder = Seq("jq", "-s", ".") ++ inputs.value.map { file =>
      file.path.path
    }

    ProcessUtils.execute(cmd1 #> output.asFile)
    output
  }
}


/**
  * Get CSV file from arrays
  */
@WeaveFunction class GetCSVFromJson {
  def apply(input: JsonFile): CSVFile = {
    val output = CSVFile.output()
    val cmd: ProcessBuilder = Seq("jq", "-r", ". | @csv")
    ProcessUtils.execute(cmd #< input.asFile #> output.asFile)
    output
  }
}
