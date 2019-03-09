package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.macrodef._

import scala.reflect.runtime.universe._

/**
  * Weave function for selecting minimizer
  */
@WeaveFunction class PickMinimizer[A <: Plannable] {
  def apply(vals: WeaveSeq[p.BigDecimal], files: WeaveSeq[A]): A = {
    val ((minscore, minfile), minidx) =
      (vals.value zip files.value).zipWithIndex.minBy { case ((pv, pf), idx) =>
        pv.value
      }
    println(s"Minimizer was ${minscore} at ${minidx}")
    minfile
  }
}

/**
  * Weave function for selecting maximizer
  */
@WeaveFunction class PickMaximizer[A <: Plannable] {
  def apply(vals: WeaveSeq[p.BigDecimal], files: WeaveSeq[A]): A = {
    val ((maxscore, maxfile), maxidx) =
      (vals.value zip files.value).zipWithIndex.maxBy { case ((pv, pf), idx) =>
        pv.value
      }
    println(s"Maximizer was ${maxscore} at ${maxidx}")
    maxfile
  }
}
