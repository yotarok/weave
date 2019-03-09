package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.macrodef._

import sys.process._
import scala.util.matching.Regex
import scala.reflect.runtime.universe.TypeTag

/**
  * Utility functions for handling TextFile input and output
  */
object TextFunctionUtility {
  def outputLineIterator(iter: Iterator[String]): p.TextFile = {
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      for (l <- iter) {
        out.write(l + "\n")
      }
    }
    output
  }
}

/** Transform text file line-by-line
  *
  * @param func transform function
  * @param text input text file
  */
@WeaveFunction class MapLines {
import TextFunctionUtility._
  def apply(
    func: Function1[p.String, p.String], text: p.TextFile)
      : p.TextFile = {
    outputLineIterator(
      text.lineIterator().map({ l => func(l).value })
    )
  }
}

/** Apply regular expression substitution to each line
  *
  * @param pattern regular expression pattern
  * @param repl replacement string
  * @param text input text file
  */
@WeaveFunction class SubstituteRegex {
  import TextFunctionUtility._
  def apply(
    pattern: p.String, repl: p.String, text: p.TextFile)
      : p.TextFile = {
    val pat = pattern.value.r
    outputLineIterator(
      text.lineIterator().map({ l => pat.replaceAllIn(l, repl.value) })
    )
  }
}

@WeaveFunction class FilterByRegex {
  import TextFunctionUtility._
  def apply(
    pattern: p.String, invert: p.Boolean, text: p.TextFile)
      : p.TextFile = {
    val pat = pattern.value.r
    outputLineIterator(
      text.lineIterator.filter({ l =>
        val matched = ! pat.findFirstIn(l).isEmpty
        (matched ^ invert) // matched && ! invert or ! matched && invert
      }))
  }
}

/** Take only first lines of the given text file
  *
  * @param numLines number of lines to be taken
  * @param text text file to be processed
  */
@WeaveFunction class TakeHead {
  def apply(numLines: p.Int, text: p.TextFile)
      : p.TextFile = {
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      for (l <- text.path.lines().take(numLines.value)) {
        out.write(l + "\n")
      }
    }
    output
  }
}

/** Dropt first lines of the given text file
  *
  * @param numLines number of lines to be dropped
  * @param text text file to be processed
  */
@WeaveFunction class DropHead {
  def apply(numLines: p.Int, text: p.TextFile)
      : p.TextFile = {
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      for (l <- text.path.lines().drop(numLines.value)) {
        out.write(l + "\n")
      }
    }
    output
  }
}

@WeaveFunction class ConcatTextFiles {
  def apply(texts: WeaveSeq[p.TextFile]): p.TextFile = {
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      for (text <- texts.value) {
        //out.write(text.path.bytes)
        text.path.copyDataTo(out)
      }
    }
    output
  }
}

@WeaveFunction class SelectLinesByModulo {
  def apply(mod: p.Int, text: p.TextFile)
      : p.Tuple2[p.TextFile, p.TextFile] = {
    val outputSel = p.TextFile.output()
    val outputOther = p.TextFile.output()

    outputSel.openAndWrite { sel =>
      outputOther.openAndWrite { oth =>
        for ((line, linum) <- text.path.lines().zipWithIndex) {
          if (linum % mod.value == 0) {
            sel.write(line + "\n")
          } else {
            oth.write(line + "\n")
          }
        }
      }
    }
    p.Tuple2(outputSel, outputOther)
  }
}

/**
  * Split lines by modulo of the line numbers
  */
@WeaveFunction class SplitLinesByModulo {
  def apply(mod: p.Int, text: p.TextFile): WeaveArray[p.TextFile] = {
    // TO DO: This implementation is not really memory efficient...
    val strs = scala.collection.mutable.ArrayBuffer[String]()
    for (i <- (0 until mod.value)) strs += "";

    for ((line, linum) <- text.path.lines().zipWithIndex) {
      val n = linum % mod.value
      strs(n) += (line + "\n")
    }

    val outputs = for (i <- (0 until mod.value)) yield {
      val output = p.TextFile.output()
      output.path.write(strs(i))
      output
    }
    WeaveArray.fromSeq(outputs)
  }
}

/**
  * Count the number of lines in the file
  */
@WeaveFunction class CountLines {
  def apply(text: p.TextFile): p.Int = {
    var n: Int = 0
    for (_ <- text.path.lines()) {
      n += 1
    }
    n
  }
}

/**
  * Shuffle lines in the text file
  *
  * This function stores all the data on the memory, i.e. this is not
  * suitable for a very large input
  */
@WeaveFunction class ShuffleLines {
  def apply(seed: p.Int, text: p.TextFile): p.TextFile = {
    val rnd = new scala.util.Random(seed.value)
    val shuffled = text.path.lines().toSeq
      .map(x => (rnd.nextDouble, x))
      .sortBy(_._1)
      .map(_._2)
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      for (line <- shuffled) {
        out.write(line + "\n")
      }
    }
    output
  }
}

/** Helper class for supporting OO-friendly notation for textfile ops*/
class TextFileOps[A <: p.TextFile : TypeTag](text: Plan[A]) {
  /** Filter by regex */
  def filterByRegex(pattern: Plan[p.String], invert: Plan[p.Boolean]) =
    FilterByRegex(pattern, invert, text)
  def substituteRegex(pattern: Plan[p.String], repl: Plan[p.String]) =
    SubstituteRegex(pattern, repl, text)
}
