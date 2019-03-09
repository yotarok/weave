package ro.yota.weave.test

import org.scalatest.WordSpec

import ro.yota.weave._
import ro.yota.weave.macrodef.WeaveFunction
import ro.yota.weave.{planned => p}

@WeaveFunction class Mul2 {
  def apply(x: p.Int): p.Int = x.value * 2
}

class ProcedureSpec extends WordSpec {
  "WeaveFunction" should {
    val proc = Mul2(2)
    "instantiated as Launchable" in {
      assert(proc.isInstanceOf[Launchable])
    }
  }

  val const_seven = Constant[p.Int](7)

  "Constant" should {
    "have an expected digest" in {
      assert(const_seven.spec.signature ==
        "ro.yota.weave.Constant[ro.yota.weave.planned.Int](7)")
      // computed as
      // $ echo -n "ro.yota.weave.Constant[ro.yota.weave.planned.Int](7)" \
      //    | shasum
      assert(const_seven.spec.digest ==
        "8648ac3151825cec07c3a022bb4ba8db97ddfce2")
    }
  }

  val two_by_seven = Mul2(const_seven)
  "Proedure made with a funciton" should {
    "have an expected digest" in {
      assert(two_by_seven.spec.signature ==
        "ro.yota.weave.test.Mul2(8648ac3151825cec07c3a022bb4ba8db97ddfce2)")

      assert(two_by_seven.spec.digest ==
        "92b050e5502b98219b69bd109ff32f8d64eb72ba")

    }
  }
}
