package ro.yota.weave.planned

import ro.yota.weave._

import scalax.file.Path
import scalax.io.Output
import scala.language.implicitConversions

trait FileCompanionBase[A <: File] {
  /**
    * Generate instance for output object
    *
    * In default, this fall back to temporary.
    */
  def output(): A = temporary()

  /**
    * Generate instance for temporary object
    */
  def temporary(): A
}

/** A plannable class referring a specific file */
class File(val path: Path)
    extends Primitive {
  type BaseType = Path
  override def value:Path = path

  val className = getTypeTag[File](this).tpe.toString
  def this() =
    this(Path.createTempFile()) // is it really required?


  override def write(output: Output) {
    output.write(path.bytes)
  }

  def asFile: java.io.File =
    new java.io.File(path.path)

  def openAndWrite[A](op: (Output=>A)) {
    for {
      processor <- this.path.outputProcessor
      out = processor.asOutput
    } {
      op(out)
    }
  }

  def preferredExt: Option[java.lang.String] = None


  override def contentDigest: java.lang.String = Hasher(this.path)

  override val contentType: java.lang.String = "application/octet-stream"

  override def show() {
    import sys.process._
    val pagerBin = sys.env.getOrElse("PAGER", "more")
    (Seq("cat", this.path.path) #| Seq("sh", "-c", pagerBin + " > /dev/tty")).!;
  }

}

object File extends FileCompanionBase[File] {
  // Create new temporary file for storing output
  def temporary(): File = {
    new File(Path.createTempFile())
  }
}

class TextFile(p: Path) extends File(p) {
  override val className = getTypeTag[TextFile](this).tpe.toString
  override val isText = true
  override val contentType = "text/plain"
  override val preferredExt = Some(".txt")

  def lineIterator(): Iterator[java.lang.String] = {
    path.lines().toIterator
  }
}

object TextFile extends FileCompanionBase[TextFile] {
    // Create new temporary file for storing output
  def temporary(): TextFile = {
    new TextFile(Path.createTempFile())
  }

  implicit def toStringPlan(ref: Plan[TextFile]): Plan[String] = {
    ref.as[String]
  }

}

class GzFile(p: Path) extends File(p) {
  override val className = getTypeTag[GzFile](this).tpe.toString
}

object GzFile extends FileCompanionBase[GzFile] {
  // Create new temporary file for storing output
  def temporary(): GzFile = {
    new GzFile(Path.createTempFile())
  }
}
