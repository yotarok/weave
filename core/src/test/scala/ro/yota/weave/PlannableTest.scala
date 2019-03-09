package ro.yota.weave.test


import ro.yota.weave._
import ro.yota.weave.storage._
import scalax.file.Path
import play.api.libs.json._
import play.api.libs.json.Json._
import org.scalatest.WordSpec
import ro.yota.weave.{planned => p}
import scala.reflect.runtime.universe._
import scalax.io.Input


class TupleSpec extends WordSpec {
    def fixture = new {
    val storage = FSStorageBuilder().withTempDirectory.build()
    val serializer = new Serializer(storage, this.getClass.getClassLoader)
  }

  "Tuple" should {
    "be serializable" in {
      val f = fixture

      val tuple1 = p.Tuple2[p.String, p.Int](new p.String("abc"), new p.Int(123))
      val key1 = "1234500000000000000000000000000000000000"

      f.serializer.storeObject(key1, tuple1, Json.obj())

      val loadedTuple1 =
        f.serializer.loadObject(key1).asInstanceOf[p.Tuple2[p.String, p.Int]]
      assert(loadedTuple1._1.value == "abc")
      assert(loadedTuple1._2.value == 123)
    }
  }

}
