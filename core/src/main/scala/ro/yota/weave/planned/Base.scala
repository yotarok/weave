package ro.yota.weave.planned

import ro.yota.weave._
import sys.process._
import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import play.api.libs.json._
import play.api.libs.json.Json._

/** Base trait that defines common operators for "primitive" planned types
  *
  * "Primitive" here means that the type is represented by a single file
  */
trait Primitive extends Plannable {
  override def toString: java.lang.String = value.toString
  override def contentDigest: java.lang.String = Hasher(this.toString)
  val isPrimitive = true
  override val isText = true
  val contentType = "text/plain"
  override def show() {
    val pagerBin = sys.env.getOrElse("PAGER", "more")
    (pagerBin #< (
      new ByteArrayInputStream(this.toString.getBytes(Charset.forName("UTF-8"))))
    ).!
  }

  def updateField(key: java.lang.String, value: Plannable): Plannable = {
    throw new RuntimeException("Attempted to update field value of a primitive object")
  }

}

/** Base trait for container types that contain multiple fields
  */
trait Container extends Plannable {
  val isPrimitive = false
  override def contentDigest: java.lang.String = {
    val source = (this.className + "\n"
      + this.fieldNames.sorted.map(this.field(_).contentDigest).mkString("\n"))
    println(s"Computing hash of container object ${source}")
    Hasher(source)
  }
  def contentType: java.lang.String = "null"
}

