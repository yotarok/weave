package ro.yota.weave.test

import org.scalatest.WordSpec

import ro.yota.weave._
import ro.yota.weave.{planned => p}

class TestProject extends Project {
  val six = Mul2(3)
  val twelve = planWith(trace = "more than 10") {
    Mul2(six)
  }
  val (fourtyeight, twentyfour) = planWith(trace = "huge") {
    val largenum = planWith(trace = "large") {
      Mul2(twelve)
    }
    (Mul2(largenum), largenum)
  }
}

class ProjectSpec extends WordSpec {
  "Project.withPlan" should {
    "provide a trace message to instantiated procedures" in {
      val proj = new TestProject()
      proj.fourtyeight match {
        case proc: Procedure[_] => {
          assert(proc.traceMessages == Seq("huge"))
        }
        case _ => fail()
      }
      proj.twentyfour match {
        case proc: Procedure[_] => {
          assert(proc.traceMessages == Seq("huge", "large"))
        }
        case _ => fail()
      }

    }
  }
}
