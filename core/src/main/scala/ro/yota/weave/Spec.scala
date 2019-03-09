package ro.yota.weave

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import play.api.libs.json._
import play.api.libs.json.Json._
import scalax.io.{Output, Input, Codec}
import scalax.file.Path

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import scala.language.postfixOps

/**
  * Spec is a node of computation graph
  */
trait Spec {
  /**
    * Signature of the object refered by this spec
    */
  def signature : String;
  /**
    * Depending objects
    */
  def depends: Seq[Plan[Plannable]];
  /**
    * Digest of the signature
    */
  def digest: String = Hasher(signature)

  final private[this] val shortDigestLen: Int = 8

  /**
    * Prefix of the digest
    */
  def shortDigest: String = digest.substring(0, shortDigestLen)
}

/**
  * Spec that has a value, e.g. Procedure, Field
  */
class ValueSpec(val signature: String, val depends: Seq[Plan[Plannable]]) extends Spec {

  override final val digest: String = Hasher(signature)
  override def toString: String = "ValueSpec(signature = " + signature + ", digest = " + digest + ")"
}

/**
  * Spec for procedures
  */
class ProcedureSpec(s: String, d: Seq[Plan[Plannable]]) extends ValueSpec(s, d) {
  override def toString: String = "ProcedureSpec(signature = " + signature + ", digest = " + digest + ")"
}

/**
  * Spec for collapsed procedures
  *
  * Signature is retained from the rootSpec not to change the object digest
  */
class CollapsedProcedureSpec(rootSpec: ProcedureSpec, d: Seq[Plan[Plannable]]) extends ValueSpec(rootSpec.signature, d) {
  override def toString: String = "CollapsedProcedureSpec(signature = " + signature + ", digest = " + digest + ")"
}

/**
  * Spec for field accessor
  */
class FieldSpec(parent: Plan[Plannable], field: String)
//extends ValueSpec(parent.digest + "." + field, parent.depends) {
    extends ValueSpec(parent.spec.digest + "." + field, Seq(parent)) {
}

/**
  * Spec that directly specifies digest
  */
class BareSpec(override val digest: String) extends Spec {
  def signature: String = {
    throw new RuntimeException("Cannot access signature of BareSpec")
  }
  def depends: Seq[Plan[Plannable]] = Seq()
}
