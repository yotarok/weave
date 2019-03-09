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
import ro.yota.weave.macrodef._

import scala.language.reflectiveCalls

@WeaveFunction(macroDebug=true) class ToLowerCase {
  def apply(s: p.String): p.String = {
    s.value.toLowerCase()
  }
}

@WeaveFunction class Id[T <: Plannable] {
  def apply(a: T): T = a
}


class FunctionSpec extends WordSpec {

  def fixture = new {
    val storage = FSStorageBuilder().withTempDirectory.build()
    val serializer = new Serializer(storage, this.getClass.getClassLoader)
  }

  "Function" should {
    "be serializable" in {
      val f = fixture

      val func1 = new ToLowerCase()
      val key1 = "1234000000000000000000000000000000000001"

      f.serializer.storeObject(key1, func1, Json.obj())
      val loadedFunc1 = f.serializer.loadObject(key1).asInstanceOf[ToLowerCase]
      assert(loadedFunc1(new p.String("ABC")).value == "abc")


      val func2 = new Id[p.String]()
      val key2 = "1234000000000000000000000000000000000000"

      f.serializer.storeObject(key2, func2, Json.obj())

      val loadedFunc2 = f.serializer.loadObject(key2).asInstanceOf[Id[p.String]]
      assert(loadedFunc2(new p.String("ABC")).value == "ABC")
    }
  }

  "AnonymousFunction" should {
    "be serializable" in {
      val f = fixture

      // The basic case
      val anonFunc1 = new AnonymousFunction1[p.String, p.String](
        "Add underscode",
        q"""(x: ro.yota.weave.planned.String) => new ro.yota.weave.planned.String(x + "_")"""
      )
      val key1 = "2234000000000000000000000000000000000000"

      f.serializer.storeObject(key1, anonFunc1, Json.obj())

      val loadedAnonFunc1 =
        f.serializer.loadObject(key1)
          .asInstanceOf[Function1[p.String, p.String]]
      println("Result == " + loadedAnonFunc1("ABC"))
      assert(loadedAnonFunc1("ABC").value == "ABC_")

      // Case with implicit conversions
      val anonFunc2 = new AnonymousFunction1[p.String, p.String](
        "Add atmark",
        q"""(x: String) => x + "@" """
      )
      val key2 = "2234000000000000000000000000000000000010"

      f.serializer.storeObject(key2, anonFunc2, Json.obj())

      val loadedAnonFunc2 =
        f.serializer.loadObject(key2)
          .asInstanceOf[Function1[p.String, p.String]]
      println("Result == " + loadedAnonFunc2("ABC"))
      assert(loadedAnonFunc2("ABC").value == "ABC@")

      val anonFunc3: Function1[p.String, p.String] =
        Lambda.lambda("Add hyphen", (x: String) => x + "-")
      val key3 = "2234000000000000000000000000000000000011"

      f.serializer.storeObject(key3, anonFunc3, Json.obj())

      val loadedAnonFunc3 =
        f.serializer.loadObject(key3)
          .asInstanceOf[Function1[p.String, p.String]]
      println("Result == " + loadedAnonFunc3("ABC"))
      assert(loadedAnonFunc3("ABC").value == "ABC-")

    }
  }

}
