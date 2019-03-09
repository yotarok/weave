package ro.yota.weave.planned

import ro.yota.weave._

import scalax.file.Path
import scalax.io.Output
import play.api.libs.json.{Json}

import sys.process._
import scalax.io.{Output, Input, Codec}

/** A plannable type for directories */
class Directory(val path: Path)
    extends Primitive {
  type BaseType = Path
  override def value: Path = path

  val className = getTypeTag[Directory](this).tpe.toString

  def this(classLoader: ClassLoader, input: Input) {
    this {
      val path = Path.createTempDirectory()
      val inp = new java.io.ByteArrayInputStream(input.byteArray);
      (Seq("tar", "-C", path.path, "-xv") #< inp).!!;
      path
    }
  }

  lazy val archivePath: Path = {
    val tempFile = Path.createTempFile();
    // TO DO: Directory should be in 7zip format because tar doesn't support
    //        directory indexing
    Seq("tar", "-C", this.path.path, "-cf", tempFile.path, ".").!!
    tempFile
  }

  override def write(output: Output) {
    output.write(archivePath.bytes)
  }

  override def contentDigest: java.lang.String = {
    Hasher(archivePath)
  }

}

object Directory {
  // Create new temporary file for storing output
  def output(): Directory = {
    new Directory(Path.createTempDirectory())
  }

  def temporary(): Directory = {
    new Directory(Path.createTempDirectory())
  }

}
