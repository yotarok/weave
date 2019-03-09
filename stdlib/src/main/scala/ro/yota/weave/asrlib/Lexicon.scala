package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._

import ro.yota.weave.macrodef._
import scalax.file.Path

import sys.process._ 

@WeaveFunction class DisambiguateLexicon {
  def apply(lexicon: p.TextFile): p.Tuple2[p.TextFile, WeaveArray[p.String]] = {
    val output = p.TextFile.output()

    var setDisambs = Set[String]()
    val pron2word = lexicon.path.lines().map { line =>
      val s = line.split(raw"\s+")
      (s.tail.toSeq, s.head)
    }.foldLeft(Map[Seq[String], Seq[String]]()) { case (map, (phons, word)) => {
      if (map contains phons) {
        map + (phons -> (map(phons) ++ Seq(word)))
      } else {
        map + (phons -> Seq(word))
      }
    }}

    val word2pron = pron2word.map({ case (phons, words) => {
      if (words.size == 1) {
        Seq(Tuple2(words.head, phons))
      } else {
        for (i <- 0 until words.size) yield {
          val disambphon = s"#${i}"
          setDisambs += disambphon
          Tuple2(words(i), phons ++ Seq(disambphon))
        }
      }
    }}).toSeq.flatten.sortBy(_._1)

    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    } {
      for ((word, phons) <- word2pron) {
        val pron = phons.mkString(" ")
        out.write(s"${word}\t${pron}\n")
      }
    }

    p.Tuple2(output, WeaveArray.fromSeq(setDisambs.toSeq.sorted.map(new p.String(_))))
  }
}

@WeaveFunction class MakePhones2WordFSTSource {
  def apply(lexicon: p.TextFile, epsSymbol: p.String): p.TextFile = {
    val output = p.TextFile.output()
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    } {
      val initst = 0
      val finst = 1
      var newst = 2

      //println(s"lines.size: ${lexicon.path.lines().size}")
      for (line <- lexicon.path.lines()) {
        val s = line.split(raw"\s+")
        val word = s.head
        val phons = s.tail.toSeq
        var prevst = initst

        for ((p, loc) <- phons.zipWithIndex) {
          val nextst = if (loc == phons.size - 1) {
            finst
          } else {
            val s = newst
            newst += 1
            s
          }

          val outSym = if (loc == 0) { word } else { epsSymbol }

          out.write(s"${prevst}\t${nextst}\t${p}\t${outSym}\n")
          prevst = nextst
        }
      }
      out.write(s"${finst}\n")
    }
    output
  }
}

@WeaveFunction class FilterLexicon {
  def apply(lexicon: p.TextFile, vocab: p.TextFile): p.TextFile = {
    val output = p.TextFile.output()
    val wordSet = vocab.path.lines().map({ line => line.split(raw"\s+")(0) }).toSet
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    } {
      for (line <- lexicon.path.lines()) {
        val word = line.split(raw"\s+")(0)
        if (wordSet.contains(word)) {
          out.write(line + "\n")
        }
      }
    }
    output
  }
}
