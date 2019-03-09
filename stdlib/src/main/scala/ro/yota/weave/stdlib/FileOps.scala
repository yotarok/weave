package ro.yota.weave.stdlib

import ro.yota.weave._
import ro.yota.weave.{planned => p}
import ro.yota.weave.macrodef._
import sys.process._

/** Import file from worker's file system or http servers
  */
@WeaveFunction
class ImportFile {
  def apply(s: p.String): p.File = {
    val output = p.File.output();
    if (s.startsWith("http://") || s.startsWith("https://")) {
      (new java.net.URL(s) #> output.asFile).!!;
    } else {
      (new java.io.File(s) #> output.asFile).!!;
    }
    output
  }
}

/** Import tar archive as a directory
  */
@WeaveFunction
class UnarchiveTar {
  def apply(tar: p.File, compressOpt: p.String): p.Directory = {
    val output = p.Directory.output()
    Seq("tar", "-C", output.path.path, "-xv" + compressOpt.value + "f", tar.path.path).!!
    output
  }
}

/** Apply gunzip
  */
@WeaveFunction
class Gunzip {
  def apply(file: p.File): p.File = {
    val output = p.File.output()
    (Seq("cat", file.path.path) #| Seq("zcat") #> output.asFile).!!
    output
  }
}
