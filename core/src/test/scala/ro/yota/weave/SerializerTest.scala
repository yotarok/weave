package ro.yota.weave.test

import ro.yota.weave._
import ro.yota.weave.storage._
import scalax.file.Path
import play.api.libs.json._
import play.api.libs.json.Json._
import org.scalatest.WordSpec
import ro.yota.weave.{planned => p}

/* There are some overlap in test coverage between this file and
 * PlannableTest.scala.
 * This file should be more aiming at the white-box testing, where
 * PlannableTest should only cover basic serialization functionality
 * with high-level API
 */

class SerializerSpec extends WordSpec {
  val storage = FSStorageBuilder().withTempDirectory.build()

  val serializer = new Serializer(storage, this.getClass.getClassLoader)

  "Serializer" should {
    "be able to serialize primitives" in {
      val key1 = "28efc90cadbaf07a3f369e081a7e79a940e7c8e8"
      val pint = new p.Int(123)
      serializer.storePrimitiveObject(
        key1, pint, Json.obj())

      val loaded = serializer.loadObject(key1)
      assert(loaded.value == pint.value)
    }
  }
}
