package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.macrodef._


/**
  * Assertion for test
  */
@WeaveFunction class AssertEq[A <: Plannable] {
  def apply(expected: A, actual: A, successHead: p.String, failHead: p.String): p.String = {
    if (expected.value != actual.value) {
      failHead + s" ${expected.toString} not equals to ${actual.toString}"
    } else {
      successHead + s"${expected.toString} equals to ${actual.toString}"
    }
  }
}

/**
  * Assertion for test
  */
@WeaveFunction class AssertEqKey[A <: Plannable] {
  def apply(expected: A, actual: A, successHead: p.String, failHead: p.String): p.String = {
    if (expected.objectKey != actual.objectKey) {
      failHead +
        s"${expected.objectKey.toString} equals to ${actual.objectKey.toString}"
    } else {
      successHead +
        s" ${expected.objectKey.toString} equals to ${actual.objectKey.toString}"
    }
  }
}
