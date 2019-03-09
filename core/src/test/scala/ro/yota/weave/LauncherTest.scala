package ro.yota.weave.test

import ro.yota.weave._
import ro.yota.weave.{planned => p}
import ro.yota.weave.storage._
import scalax.file.Path
import play.api.libs.json._
import play.api.libs.json.Json._
import org.scalatest.WordSpec

import scala.language.reflectiveCalls
import com.typesafe.scalalogging.Logger

case class WeaveConfigBuilder(val config: JsObject = Json.obj()) {
  def withStorageConfig(storageConfig: JsObject) = {
    WeaveConfigBuilder(config ++ Json.obj("storage" -> storageConfig))
  }

  def buildLauncher() = {
    new Launcher(config)
  }
}

class LauncherSpec extends WordSpec {
  implicit val logger = Logger(classOf[LauncherSpec])

  def fixture = new {
    val launcher = WeaveConfigBuilder()
      .withStorageConfig(FSStorageBuilder().withTempDirectory.config)
      .buildLauncher()
    val serializer = new Serializer(launcher.storage, Seq())
  }

  class ConstInt(private val i: Int) extends Function0[p.Int] {
    final val className = scala.reflect.runtime.universe.typeTag[ConstInt].tpe.toString
    def apply(): p.Int = new p.Int(i)
  }

  class Add extends Function2[p.Int, p.Int, p.Int] {
    final val className = scala.reflect.runtime.universe.typeTag[Add].tpe.toString
    def apply(x: p.Int, y: p.Int): p.Int = new p.Int(x.value + y.value)
  }

  "Launcher" should {
    "be able to launch functions" in {
      val f = fixture
      var stage = ObjectStage(f.serializer)
      val func1 = new ConstInt(3)
      val outKey1 = "0000000000000000000000000000000000000000"
      stage = f.launcher.launchFunction(
        stage, func1, Seq(), outKey1, Json.obj(),
        false)
      assert(stage.objectPool contains outKey1)
      assert(stage.objectPool(outKey1).value == 3)

      val func2 = new Add()
      val outKey2 = "0000000000000000000000000000000000000001"
      stage = f.launcher.launchFunction(
        stage, func2, Seq(outKey1, outKey1), outKey2, Json.obj(),
        false)
      assert(stage.objectPool contains outKey2)
      assert(stage.objectPool(outKey2).value == 6)
    }

    "use cache if possible" in {
      val f = fixture
      var stage = ObjectStage(f.serializer)
      val func1 = new ConstInt(3)
      val outKey1 = "0000000000000000000000000000000000000000"
      stage = f.launcher.launchFunction(
        stage, func1, Seq(), outKey1, Json.obj(),
        false)

      val cacheKey1 = func1.cacheKey(Seq())
        .getOrElse(fail("cachekey must be defined"))

      val cacheMap = stage.newCacheLinks.toMap
      assert(cacheMap contains cacheKey1)
      assert(cacheMap(cacheKey1) == outKey1)

      stage.flush()

      val func2 = new ConstInt(4)
      // This test abuses the intentional bug that ConstInt doesn't take
      // the argument into account for computing cacheKey.
      // func2 and func1 have the same cacheKey, thus the output will be 3
      // if cache is used.
      val outKey2 = "0000000000000000000000000000000000000001"
      stage = f.launcher.launchFunction(
        stage, func2, Seq(), outKey2, Json.obj(),
        false)

      assert(stage.objectPool contains outKey2)
      assert(stage.objectPool(outKey2).value == 3)
    }

    "not launch a function if the output already exists" in {
      val f = fixture
      var stage = ObjectStage(f.serializer)
      val func1 = new ConstInt(3)
      val outKey1 = "0000000000000000000000000000000000000000"
      stage = f.launcher.launchFunction(
        stage, func1, Seq(), outKey1, Json.obj(),
        false)

      stage.flush()
      
      val func2 = new ConstInt(4)
      stage = f.launcher.launchFunction(
        stage, func2, Seq(), outKey1, Json.obj(),
        false) // try to write different object to the same key

      assert(stage.objectPool contains outKey1)
      assert(stage.objectPool(outKey1).value == 3)
    }
  }
}
