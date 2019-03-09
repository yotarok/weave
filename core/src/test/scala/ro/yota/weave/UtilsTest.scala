package ro.yota.weave.test

import org.scalatest.WordSpec

import ro.yota.weave._

class EvalSpec extends WordSpec {
  "Eval.evalMany" should {
    "evaluate expressions" in {
      assert(Eval.evalMany[Integer](this.getClass.getClassLoader(),
        Seq("2 + 2", "2 - 2")) == Seq(4, 0))
    }
  }
}

class PrettifierSpec extends WordSpec {
  "Prettifier.truncate" should {
    "truncate strings with ellipses" in {
      assert (Prettifier.truncate("Hello World", 10) == "Hello ... ")
    }
    "not truncate if the string is shorter than the target length" in {
      assert (Prettifier.truncate("Hell", 10) == "Hell")
      assert (Prettifier.truncate("Hello", 5) == "Hello")
      assert (Prettifier.truncate("H", 2) == "H")
    }
    "throw an error when tries to truncate to the width <= 5" in {
      assertThrows[RuntimeException] {
        Prettifier.truncate("Hello", 2)
      }
    }

  }

  "Prettifier.shortenClassName" should {
    "truncate class names in the type name" in {
      assert(Prettifier.shortenClassName("pa.ck.age.Class") == "Class")
      assert(Prettifier.shortenClassName(
        "pa.ck.age.Class[an.ot.her.ClassA, ClassB]") == "Class[ClassA,ClassB]")
    }
  }

  "Prettifier.prettifyFunctionSignature" should {
    "shorten function signature" in {
      val sign1 = """pa.ck.age.Func1[pa.ck.T](
3f786850e387550fdab836ed7e6dc881de23001b,
89e6c98d92887913cadf06b2adb97f26cde4849b)""".replace("\n", "")
      assert(Prettifier.prettifyFunctionSignature(sign1) ==
        "Func1[T](3f786850,89e6c98d)")
    }
    "ignore version string" in {
      val sign1 = """pa.Func2[A, B](123456767,1234567732)(abc)"""
      assert(Prettifier.prettifyFunctionSignature(sign1) ==
        "Func2[A,B](12345676,12345677)")
    }
  }
}

class W3CDTFSpec extends WordSpec {
  "W3CDTF.format" should {
    "format date object properly" in {
      val dtf = W3CDTF.format(new java.util.Date(1234567890123L),
        Some("GMT"))
      assert(dtf.startsWith("2009-02-13T23:31:30"))
    }
  }
}

class JsonOpsSpec extends WordSpec {
  "JsonOps.hideSecretInfo" should {
    import play.api.libs.json.{Json, JsObject, JsArray, JsValue, JsString}
    import scala.util.matching.Regex

    "be able to hide keys" in {
      val jsobj = Json.parse("""{"abc": "def", "_def": [1, 2, 3]}""")
      assert(JsonOps.hideSecretInfo("^_.*$".r, jsobj).toString ==
        """{"abc":"def","_def":"__hidden__"}""")
    }

    "be able to hide keys recursively" in {
      val jsobj = Json.parse("""
{"abc":[{"_hij":3},{},{"_klm":4},{"nop":6}], "_def": [1, 2, 3]}""")
      assert(JsonOps.hideSecretInfo("^_.*$".r, jsobj, "H").toString ==
        """{"abc":[{"_hij":"H"},{},{"_klm":"H"},{"nop":6}],"_def":"H"}""")

    }

  }
}
