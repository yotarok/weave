package ro.yota.weave.test

import ro.yota.weave._
import ro.yota.weave.storage._
import scalax.file.Path
import play.api.libs.json._
import play.api.libs.json.Json._
import org.scalatest.WordSpec
import slick.jdbc.SQLiteProfile.api._
import scala.concurrent._
import scala.concurrent.duration._

case class JournalBuilder(config: JsObject = Json.obj()) {
  def withTempFile = {
    val path = scalax.file.FileSystem.default.createTempFile()
    JournalBuilder(config ++ Json.obj("path" -> path.path))
  }

  def withFile(path: String) = {
    JournalBuilder(config ++ Json.obj("path" -> path))
  }

  def build() = {
    Journal.createJournal(config)
  }
}

class JournalSpec extends WordSpec {
  val tempDir = scalax.file.FileSystem.default.createTempDirectory()
  System.setProperty("java.io.tmpdir", tempDir.path)
  // work around for a JVM design issue that JVM doesn't allow
  // loading the same JNI library from the same path.

  val builder = JournalBuilder().withTempFile
  val dbpath = Path.fromString((builder.config \ "path").as[String])
  val journal = builder.build()

  "Journal" should {
    "write order information" in {


      journal.writeOrders("0", "0",
        Seq(
          "object.variable_x",
          "object.variable_y",
          "object.variable_y"
        ),
        Seq(
          "0000000000000000000000000000000000000000",
          "0000000000000000000000000000000000000001",
          "0000000000000000000000000000000000000002"
        ),
        Seq(
          "000000000000000000000000000000000000000a",
          "000000000000000000000000000000000000000b"
        )
      )

      val q = for {
        order <- JournalDatabase.orders;
        if order.name === "object.variable_y"
      } yield order.digest
      val check = Await.result(journal.database.run(q.result), Duration.Inf)
      assert (check.toSet == Set(
        "0000000000000000000000000000000000000001",
        "0000000000000000000000000000000000000002"))
    }


  }
}
