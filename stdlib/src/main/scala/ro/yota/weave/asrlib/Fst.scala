package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.{doclib => doc}

/** This only supports textual composition */
case class MakeSimpleReplacementFST(
  data: Iterable[(String, String)],
  arcType: String,
  inputEpsSymbol: Plan[p.String] = "<eps>",
  outputEpsSymbol: Plan[p.String] = "<eps>"
) extends Project {

  val source = ((for ((s1, s2) <- data) yield {
    s"0 0 ${s1} ${s2}"
  }).mkString("\n") + s"\n0\n").as[p.TextFile]
  val inputSymTable = fst.ExtractInputSymbolsFromSource(source,
    WeaveArray(inputEpsSymbol))
  val outputSymTable = fst.ExtractOutputSymbolsFromSource(source,
    WeaveArray(outputEpsSymbol))
  val compileOpts = fst.CompileFSTOpts().copy(
    "keepISymbols" -> true, "keepOSymbols" -> true, "arcType" -> arcType)

  val builtFST =
    fst.CompileFST(compileOpts, source, inputSymTable, outputSymTable)
}

/**
  * Subproject for making lexicon FST
  *
  * @param lexicon Text file containing lexicon in standard lexicon format
  * @param silenceConfig Pairs of non-speech phones and corresponding costs
  */
case class MakeLexiconFST(
  lexicon: Plan[p.TextFile],
  silenceConfig: Seq[(String, BigDecimal)]
) extends Project {
  val disamb = DisambiguateLexicon(lexicon)

  /** Disambiguated lexicon in the standard lexicon format */
  val disambLexicon = disamb._1

  /** List of disambiguation phone symbols */
  val disambPhones = disamb._2

  val disambPhonesStr = JoinString(disambPhones, ",")

  /** Source of the FST without loop and silence insertion */
  val singleWordFSTSrc = MakePhones2WordFSTSource(disambLexicon, "<eps>")

  val silenceSymbols = silenceConfig.map(_._1)

  val assignedInputSymbols = (Seq("<eps>") ++ silenceSymbols).map(
    convertFromString(_))

  val lexFSTInputSymbols = fst.ExtractInputSymbolsFromSource(
    singleWordFSTSrc, WeaveArray(assignedInputSymbols:_*))

  val lexFSTOutputSymbols = fst.ExtractOutputSymbolsFromSource(
    singleWordFSTSrc, WeaveArray("<eps>"))

  val phones = FilterByRegex(
    "((^#)|(^<eps>))", true,
    SubstituteRegex(raw"\s+.*$$", "", lexFSTInputSymbols)
  )


  /** Source of the FST for silence insertion */
  val shortPauseFSTSrc = silenceConfig.map( { case (sym, weight) =>
    s"0\t1\t${sym}\t<eps>\t${weight.toString}\n"
  }).mkString + "1\n"

  val singleWordFST = fst.CompileFST(
    fst.CompileFSTOpts().copy("keepISymbols" -> true, "keepOSymbols" -> true),
    singleWordFSTSrc, lexFSTInputSymbols, lexFSTOutputSymbols)

  val shortPauseFST = fst.CompileFST(
    fst.CompileFSTOpts().copy("keepISymbols" -> true, "keepOSymbols" -> true),
    convertFromString(shortPauseFSTSrc).as[p.TextFile],
    lexFSTInputSymbols, lexFSTOutputSymbols)

  val optSingleWordFST = fst.OptimizeFST(singleWordFST)

  /** 
    * Final product 
    */
  val lexFST = fst.ConcatFST(
    shortPauseFST,
    fst.GetClosureFST(fst.ConcatFST(optSingleWordFST, shortPauseFST), true))

}

/**
  * Subproject for estimating an N-gram FST from the text
  * 
  * @param text Input text
  * @param order N for N-gram
  */
case class EstimateNGramFST(text: Plan[p.TextFile], order: Int = 3)
    extends Project {
  val vocabulary =
    grm.ExtractSymbols(grm.SymbolTableOpts().copy("epsSymbol" -> "<eps>"),
      text)

  val trainTextFAR = grm.CompileStrings(grm.CompileStringsOpts(),
    vocabulary, text)

  val count = grm.CountNGram(grm.CountNGramOpts(), order, trainTextFAR)

  /** 
    * Final product: An estimated N-gram model in the ARPA format
    */
  val gramFST = grm.MakeNGramModel(grm.MakeNGramModelOpts(), "kneser_ney", count)

}
