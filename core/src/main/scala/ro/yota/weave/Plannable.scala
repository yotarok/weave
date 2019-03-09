package ro.yota.weave

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import play.api.libs.json._
import play.api.libs.json.Json._
import scalax.io.{Output, Input, Codec, Resource}
import scalax.file.Path

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.language.postfixOps
import ro.yota.weave.{planned => p}

// Plannables are types that can be planned
// Plannable must support basic serialization

trait Plannable {
  type BaseType;

  // default implementation for non-primitive types
  def write(output: Output) { Plannable.doNotCallWrite }

  /**
    * Field to store the object key if the object is loaded from storage
    */
  var objectKey: Option[String] = None

  /**
    * Field to store the content digest if the object is loaded from storage
    */
  var loadedContentDigest: Option[String] = None

  /**
    * Field to store the object key when the object is to be stored in the storage
    */
  var outputKey: Option[String] = None

  /** Set objectKey for this object and field objects
    */
  def outputKey_=(s: String) {
    outputKey = Some(s)
    for (f <- fieldNames) {
      val fieldKey = Hasher(s + "." + f)
      field(f).outputKey = f
    }
  }

  def basicInfo : JsObject = {
    Json.obj(
      "className" -> this.className,
      "isPrimitive" -> this.isPrimitive,
      "fieldNames" -> this.fieldNames,
      "contentDigest" -> this.contentDigest,
      "isText" -> this.isText,
      "contentType" -> this.contentType
    )
  }

  def isText: Boolean = false;

  def contentDigest: String;
  def value: BaseType;

  def contentType: String;

  def className : String;
  def getTypeTag[T: ru.TypeTag](obj: T): TypeTag[T] = ru.typeTag[T]

  def fieldNames : Seq[String] = List()
  def field(key: String) : Plannable = Plannable.fieldNotFound(key)
  def field(path: Seq[String]) : Plannable = {
    if (path.size == 0) {
      this
    } else {
      this.field(path.head).field(path.tail)
    }
  }

  def updateField(key: String, value: Plannable): Plannable;

  def isPrimitive : Boolean

  /**
    * Do validation on file format
    */
  def isValid(): Boolean = true

  def show() {
    println("Cannot show object of this type")
  }
}

object Plannable {
  def doNotCallConstructorWithInput[A]: A = {
    throw new RuntimeException("Don't call input constructor of complex data types")
  }
  def doNotCallWrite[A]: A = {
    throw new RuntimeException("Don't call write method of nonprimitive data")
  }
  def doNotCallValue[A]: A = {
    throw new RuntimeException("Don't call value method of type adapter")
  }
  def fieldNotFound[A](field: String): A = {
    throw new RuntimeException("Field not found")
  }
}

