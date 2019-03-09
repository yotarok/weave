package ro.yota.weave.stdlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.macrodef._
import scala.language.implicitConversions

import scalax.file.Path
import kantan.csv.{rfc,Success}
import kantan.csv.ops._
import scala.util.matching.Regex

@WeaveFileType class TableFile(p: Path) extends p.TextFile(p) {
  /** Return iterator for traversing rows
    */
  def toIterator: Iterator[Seq[String]] = {
    throw new RuntimeException("Shouldn't be called");
  }
  def write(iter: Iterator[Seq[String]]): Unit = {
    throw new RuntimeException("Shouldn't be called");
  }
}

@WeaveFileType class CSVFile(p: Path) extends TableFile(p) {
  override def toIterator: Iterator[Seq[String]] = {
    p.fileOption match {
      case None => {
        throw new RuntimeException("The file doesn't exist, why?")
      }
      case Some(file) => {
        file.asUnsafeCsvReader[Seq[String]](rfc).toIterator
      }
    }
  }

  override def write(iter: Iterator[Seq[String]]): Unit = {
    p.fileOption match {
      case None => {
        throw new RuntimeException("The file doesn't exist, why?")
      }
      case Some(file) => {
        var writer = file.asCsvWriter[Seq[String]](rfc)
        for (vals <- iter) {
          writer = writer.write(vals)
        }
        writer.close()
      }
    }
  }
}

@WeaveFileType class TSVFile(p: Path) extends TableFile(p) {
  override def toIterator: Iterator[Seq[String]] = {
    p.lines(scalax.io.Line.Terminators.Auto, false).toIterator.map(l =>
      l.split("\t")
    )
  }

  override def write(iter: Iterator[Seq[String]]): Unit = {
    for {
      processor <- p.outputProcessor
      out = processor.asOutput
    } {
      for (row <- iter) {
        out.write(row.mkString("\t") + "\n")
      }
    }
  }
}

/** Filter table file by matching regular expression to the specified column
  *
  * @param regex regular expression string
  * @param column column number to be matched
  * @param input table to be processed
  */
@WeaveFunction class FilterRowsByRegex[A <: TableFile] {
  def apply(pattern: p.String, invert: p.Boolean, column: p.Int, input: A): A = {
    val pat = pattern.value.r
    val filteredIter = input.toIterator.filter(vals => {
      val str = vals(column.value)
      val matched = ! pat.findFirstIn(str).isEmpty
      matched ^ invert // matched && ! invert or ! matched && invert
    })

    val output = makeOutputFile[A]()
    output.write(filteredIter)
    output
  }
}

/** Filter columns from the input table by the given set of column indices
  *
  * @param cols set of columns represented in an array
  * @param invert if true, invert selection
  * @param input the input table file
  */
@WeaveFunction class FilterColsByIndex[A <: TableFile] {
  def apply(cols: WeaveArray[p.Int], invert: p.Boolean, input: A): A = {
    val mappedIter = if (invert) {
      val delColSet = cols.value.map(x => x.value).toSet
      input.toIterator.map(vals => {
        vals.zipWithIndex.filterNot({ case (_, col) => delColSet contains col}).unzip._1
      })
    } else {
      input.toIterator.map(vals =>
        cols.value.map(idx => vals(idx.value))
      )
    }

    val output = makeOutputFile[A]()
    output.write(mappedIter)
    output
  }
}

/** Concatenate columns in the table file and generate text file
  *
  * @param sep separator for columns
  * @param input the input table file
  */
@WeaveFunction class TableToText[A <: TableFile] {
  def apply(sep: p.String, input: A): p.TextFile = {
    val output = p.TextFile.output()
    output.openAndWrite { out =>
      for (vals <- input.toIterator) {
        out.write(vals.mkString(sep.value) + "\n")
      }
    }
    output
  }
}


/** Apply function to the values in the specified column
  *
  * @param func function to map string value to string value
  * @param column the number of column to be mapped
  * @param input input table file
  */
@WeaveFunction class MapColumn[A <: TableFile] {
  def apply(func: Function1[p.String, p.String], column: p.Int, input: A): A = {
    val mappedIter = {
      input.toIterator.map(vals =>
        vals.updated(column.value,
          func(vals(column.value)).value)
      )
    }

    val output = makeOutputFile[A]()
    output.write(mappedIter)
    output

  }
}

