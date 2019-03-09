package ro.yota.weave.functional_test.core
import scala.language.implicitConversions
import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._

import ro.yota.weave.macrodef._


object ConstAndFunctionTest extends Project {
  val lower = "abc"
  val upper = "ABC"
  val result1 = AssertEq(upper, ToUpperCase(lower), "SUCCESS", "FAILED").checkPoint(
    "core.ConstAndFunctionTest.result1.txt")
}

object TupleTest extends Project {
  val expected: Plan[p.Tuple2[p.String, p.String]] = ("ABC", "ABC")
  val tup: Plan[p.Tuple2[p.String, p.String]] =
    p.Tuple2(ToUpperCase("abc"), "ABC": Plan[p.String])
  val result1 = AssertEq(expected, tup, "SUCCESS", "FAILED").checkPoint(
    "core.TupleTest.result1.txt")
}

object CastTest extends Project {
  val number = Constant(p.BigDecimal("2.3"))
  val result1 = AssertEq("2.3", number.as[p.String], "SUCCESS", "FAILED").checkPoint(
    "core.CastTest.result1.txt")

  val file = WriteToFile("12345")
  val result2 = AssertEq(12345, file.as[p.Int], "SUCCESS", "FAILED").checkPoint(
    "core.CastTest.result2.txt")
}

@WeaveRecord class Person(
  val firstName: p.String,
  val lastName: p.String,
  val age: p.Int) {
  def default = {
    new Person("Jane", "Doe", 10)
  }
}

object RecordTest extends Project {
  var person = Person("Yotaro", "Kubo", 17)
  val result1 = AssertEq(person.age, 17, "SUCCESS", "FAILED").checkPoint(
    "core.RecordTest.result1.txt")
  person = person.copy("age" -> 34)
  val result2 = AssertEq(person.age, 34, "SUCCESS", "FAILED").checkPoint(
    "core.RecordTest.result2.txt")
}

object ExternalCommandTest extends Project {
  val source = Constant(new p.String("""abc
123
def
456
""")).as[p.TextFile]
  val expected = Constant(new p.String("""123
456
""")).as[p.TextFile]
  val grepped = Grep(GrepOption(), raw"^[0-9]*$$", source)
  val result1 = AssertEq(expected.as[p.String], grepped.as[p.String], "SUCCESS", "FAILED").checkPoint(
    "core.ExternalCommandTest.result1.txt")
}

object CacheTest extends Project {
  val code1 = ToUpperCase("foobar")
  val code2 = ToUpperCase(ToLowerCase("FooBar"))
  val result1 = AssertEqKey(code1, code2, "SUCCESS", "FAILED").checkPoint(
    "core.CacheTest.result1.txt")
}


object Test extends Project {
  val allAsserts: Plan[WeaveSeq[p.String]] =
    WeaveArray(
      ConstAndFunctionTest.result1,
      TupleTest.result1,
      CastTest.result1,
      CastTest.result2,
      RecordTest.result1,
      ExternalCommandTest.result1,
      CacheTest.result1
    )
  val result = JoinString(allAsserts, "\n").checkPoint(
    "core.assertions.txt")

}
