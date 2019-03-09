package exp

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._

import ro.yota.weave.macrodef._
import ro.yota.weave.macrodef.Lambda._
import scala.reflect.runtime.universe._
import scala.language.postfixOps

@WeaveFunction class DoubleSecond {
  def apply(t: p.Tuple2[p.String, p.String]): p.Tuple2[p.String, p.String] =
    t.value match {
      case Tuple2(a, b) => p.Tuple2(a, b.value + b.value)
    }
}

@WeaveFunction class DoubleString {
  override def isAuxiliary = true
  def apply(x: p.String): p.String =
    x.value + x.value
}

@WeaveFunction class HalfInt {
  def apply(i: p.Int): p.Int = i.value / 2
}


object Sample01 extends Project {
  import p.Tuple2._
  val train = "123" //Gunzip(HTTPDownload("http://hogehoge").as[GzFileRef])
  val tup2: Plan[p.Tuple2[p.String, p.String]] = p.Tuple2(train, train)

  val text: Plan[p.String] = s"""abc
def ghi
jkl mn
o p q r"""

  val uptext = MapLines(lambda("to upper", (x: String) => x.toUpperCase), text.as[p.TextFile])

  //val tup2d = new Procedure[p.Tuple2[p.String, p.String]](new UpdateField,
  //Seq(tup2, p.Tuple2("_2", DoubleString(tup2._2))))
  val tup2d = tup2.copy("_2" -> DoubleString(tup2._2))
  val dec = tup2d._2.as[p.Int]
  val halfdec = HalfInt(dec)

  val array: Plan[WeaveSeq[p.BigDecimal]] = WeaveArray(BigDecimal(3), BigDecimal(5), BigDecimal(10), BigDecimal(12))
  val maximum = PickMaximizer(array, array)

}
