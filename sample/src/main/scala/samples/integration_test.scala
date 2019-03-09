package ro.yota.exp.integration_test

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.asrlib._

import ro.yota.weave.macrodef._
import scala.reflect.runtime.universe._
import scala.language.postfixOps

import scala.util.matching._
import sys.process._ 
import java.net.URL 
import java.io.File 
import scalax.io._
import scalax.file.Path
import scala.math.BigDecimal

case class TrainingCorpus(val dataSourceUrl: String) extends Corpus {
  val feature = Seq(
    ImportFile(dataSourceUrl + "/trainFeats_0")
      .as[spn.CorpusFile]
      .checkPoint(Path.fromString(s"./checkpoint/train.feat.corpus").toAbsolute.path)
  )
    

  val wordGraph = Seq(
    ImportFile(dataSourceUrl + "/trainWords_0")
      .as[spn.CorpusFile]
      .checkPoint(Path.fromString(s"./checkpoint/train.word.corpus").toAbsolute.path)
  )
}

object IntegrationTest extends Project {
  val dataSourceUrl = "https://dl.dropboxusercontent.com/u/971009/WeaveIntegTestData"
  //val dataSourceUrl = "/Users/yotaro/Dropbox/Public/WeaveIntegTestData"
  val lexiconUrl = dataSourceUrl + "/VoxForge.tgz"

  val textUrl = dataSourceUrl + "/prompt.train.txt"
  val trainText = ImportFile(textUrl).as[p.TextFile]

  val makeNGramFST = EstimateNGramFST(trainText)

  val rawLexicon = SubstituteRegex("\\[[^\\]]*\\]", "",
    SubstituteRegex(raw"\([0-9]+\) ", " ",
      ExtractFile(UnarchiveTar(ImportFile(lexiconUrl), "z"),
        "VoxForge/VoxForgeDict").as[p.TextFile]))

  val lexicon = FilterLexicon(rawLexicon, makeNGramFST.vocabulary)

  val makeLexFST = MakeLexiconFST(lexicon,
    Seq("<eps>" -> BigDecimal("0"), "sil" -> BigDecimal("0")))

  val trainCorpus = TrainingCorpus(dataSourceUrl)
  val evalFeats = ImportFile(dataSourceUrl + "/evalFeats_0")
    .checkPoint(Path.fromString(s"./checkpoint/eval.feat.corpus").toAbsolute.path)
    .as[spn.CorpusFile]

  val evalWords = ImportFile(dataSourceUrl + "/evalWords_0")
    .checkPoint(Path.fromString(s"./checkpoint/eval.word.corpus").toAbsolute.path)
    .as[spn.CorpusFile]

  val monoTree =
    spn.MakeInitialTree(makeLexFST.phones, "sil")
      .checkPoint(Path.fromString(s"./checkpoint/monoTree.yaml").toAbsolute.path)

  val monoTrainResource = new {
    val corpus = trainCorpus
    val tree = monoTree
    val lexiconFST = makeLexFST.lexFST
  } with AMTrainingResourceWithLexicon

  val monoM1 = TrainGMMByMLE(
    monoTrainResource,
    trainCorpus.mergedTreeStats(monoTrainResource.equalAlignment, 0, 0),
    1, 39, 4)

  val triTreeStats =
    trainCorpus.mergedTreeStats(monoM1.allAligns.last, 1, 1)
  val triTree = spn.SplitTree(
    spn.BuildQuestion(monoTree, triTreeStats), triTreeStats, 400)
    .checkPoint(Path.fromString(s"./checkpoint/triTree.yaml").toAbsolute.path)

  val triTrainResource = new {
    val corpus = trainCorpus
    val tree = triTree
    val lexiconFST = makeLexFST.lexFST
  } with AMTrainingResourceWithLexicon

  val stateCtxLexFST = fst.OptimizeFST(
    spn.ComposeFST(spn.ComposeFSTOpts(),
      triTrainResource.stateContextFST, makeLexFST.lexFST))
  val decodeNet = fst.PushWeight(
    spn.ComposeFST(spn.ComposeFSTOpts().copy("lookahead" -> true),
      stateCtxLexFST, makeNGramFST.gramFST))

  val triM16 = TrainGMMByMLE(
    triTrainResource, triTreeStats,
    4, 39, 3)
  val finalGMM = triM16.allGMMs.last
    .checkPoint(Path.fromString(s"./checkpoint/triGMM").toAbsolute.path)

  val hypo = spn.Decode(spn.DecodeOpts().copy(
    "beam" -> BigDecimal("11.0"),
    "maxactive" -> 3000, "acscale" -> BigDecimal("0.1")),
    evalFeats, finalGMM, WeaveNone,
    decodeNet)
    .checkPoint(Path.fromString(s"./checkpoint/triGMM.decode").toAbsolute.path)

  val scoreAndError = spn.AlignHypothesis(hypo, "decoded", evalWords, "wordgraph")

  val wordErrorRate = spn.ExtractWER(scoreAndError._1)
    .checkPoint(Path.fromString(s"./checkpoint/triGMM.WER").toAbsolute.path)
}
