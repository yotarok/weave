package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.macrodef._
import scalax.file.Path

import sys.process._

@WeaveFunction
class ExtractFile {
  def apply(dir: p.Directory, relPath: p.String): p.File = {
    new p.File(dir.path / Path.fromString(relPath))
  }
}

/** Create new directory object from existing path
  *
  * Note that this function only works with a shared file system
  *
  * @param path path string
  */
@WeaveFunction
class ImportDirectory {
  override val version = "v2"
  def apply(path: p.String): p.Directory = {
    val output = p.Directory.output()
    ProcessUtils.execute(Seq("bash", "-c", s"cp -RL '${path.value}'/* '${output.path.path}'"))
    output
  }
}

/** Create empty directory
  */
@WeaveFunction
class CreateDirectory {
  def apply(): p.Directory = {
    p.Directory.output()
  }
}

/** Add a file to the directory */
@WeaveFunction
class AddFile {
  override def isAuxiliary = true
  def apply(destPath: p.String, file: p.File, dir: p.Directory): p.Directory = {
    val output = p.Directory.output()
    ProcessUtils.execute(Seq("bash", "-c", s"rm -r '${output.path.path}'"))
    ProcessUtils.execute(Seq("bash", "-c", s"cp -r '${dir.path.path}' '${output.path.path}'"))
    ProcessUtils.execute(Seq("bash", "-c",
      s"cp '${file.path.path}' '${output.path.path}/${destPath.value}'"))
    output
  }
}


/** Helper class for supporting OO-friendly notation for directory ops*/
class DirectoryOps(dir: Plan[p.Directory]) {
  /** Wrapper for AddFile function */
  def addFile(destPath: Plan[p.String], file: Plan[p.File]): Plan[p.Directory] =
    AddFile(destPath, file, dir)
}

