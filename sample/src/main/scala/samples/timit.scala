package exp

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._

import ro.yota.weave.macrodef._
import scala.reflect.runtime.universe._
import scala.language.postfixOps

import sys.process._ 
import java.net.URL 
import java.io.File 
import scalax.io._
import scala.math.BigDecimal
import org.yaml.snakeyaml._

import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.{doclib => doc}
import ro.yota.weave.asrlib._
import scalax.file.{Path, PathSet}

import scala.reflect.runtime.universe._
import scalax.io.Output
import scala.language.implicitConversions

object TIMITPhoneMaps {
  final val mapFullTo48 = Map(
    "ax-h" -> "ax", "axr" -> "er", "em" -> "m", "eng" -> "ng",
    "hv" -> "hh", "nx" -> "n", "ux" -> "uw", "pcl" -> "cl",
    "tcl" -> "cl", "kcl" -> "cl", "bcl" -> "vcl", "dcl" -> "vcl",
    "gcl" -> "vcl", "h#" -> "sil", "pau" -> "sil", "q" -> ""
  )
  final val map48To39 = Map(
    "aa" -> "aa", "ae" -> "ae", "ah" -> "ah", "ao" -> "aa", "aw" -> "aw",
    "ax" -> "ah", "er" -> "er", "ay" -> "ay", "b" -> "b", "vcl" -> "sil",
    "ch" -> "ch", "d" -> "d", "dh" -> "dh", "dx" -> "dx", "eh" -> "eh",
    "el" -> "l", "m" -> "m", "en" -> "n", "ng" -> "ng", "epi" -> "sil",
    "hh" -> "hh", "ih" -> "ih", "ix" -> "ih", "l" -> "l", "n" -> "n",
    "k" -> "k", "sh" -> "sh", "zh" -> "sh", "uw" -> "uw", "cl" -> "sil",
    "sil" -> "sil", "ow" -> "ow", "uh" -> "uh", "t" -> "t", "iy" -> "iy",
    "jh" -> "jh", "g" -> "g", "th" -> "th", "y" -> "y", "oy" -> "oy", 
    "ey" -> "ey", "p" -> "p", "s" -> "s", "v" -> "v", "r" -> "r",
    "z" -> "z", "w" -> "w", "f" -> "f"
  )

  val map48To39FST =
    MakeSimpleReplacementFST(TIMITPhoneMaps.map48To39, "standard").builtFST

}

@WeaveFunction class EnumerateTIMITFiles {
  override val version = "2016-02-08v7"

  def apply(rootDir: p.String, partitionName: p.String): p.TextFile = {
    val output = p.TextFile.output()
    val coreSpeakers = Set(
      "dr1_mdab0", "dr1_mwbt0", "dr1_felc0", "dr2_mtas1", "dr2_mwew0", "dr2_fpas0",
      "dr3_mjmp0", "dr3_mlnt0", "dr3_fpkt0", "dr4_mlll0", "dr4_mtls0", "dr4_fjlm0",
      "dr5_mbpm0", "dr5_mklt0", "dr5_fnlp0", "dr6_mcmj0", "dr6_mjdh0", "dr6_fmgd0",
      "dr7_mgrt0", "dr7_mnjm0", "dr7_fdhc0", "dr8_mjln0", "dr8_mpam0", "dr8_fmld0"
    )

    val subDir = if (partitionName.value == "train") { "TRAIN" } else { "TEST" }
    val subPath = Path.fromString(rootDir.value) / subDir
    val allWavs: PathSet[Path] = subPath.descendants((_:Path).name.endsWith(".WAV"))

    var idx = 0

    val data = for {
      path <- allWavs.toSeq ;
      parent <- path.parent ;
      speakerId = parent.name.toLowerCase() ;      
      sentenceId = path.name.dropRight(4).toLowerCase() ;
      grandparent <- parent.parent ;
      dialectId = grandparent.name.toLowerCase() ;
      dialectAndSpeakerId = s"${dialectId}_${speakerId}"
      phonePath = path.path.dropRight(4) + ".PHN" ;
      if {
        if (partitionName.value == "train") { true }
        else {
          if (coreSpeakers contains dialectAndSpeakerId) {
            partitionName.value == "core"
          } else { partitionName.value == "dev" }
        }
      } ;
      if ! sentenceId.startsWith("sa")
    } yield {
      val idxStr = f"$idx%05d"
      idx += 1
      s"${idxStr}_${dialectAndSpeakerId}_${sentenceId}\t${path.path}\t${phonePath}"
    }
    output.path.writeStrings(data, "\n")
    output
  }
}

@WeaveFunction class CreateTIMITWaveCorpus {
  override val version = "2016-02-09"

  def apply(fileInfo: p.TextFile): spn.CorpusFile = {
    val output = spn.CorpusFile.output()
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    }{
      for (line <- fileInfo.path.lines()) {
        val values = line.split("\t")
        val uttId = values(0)
        val uttIdComps = uttId.split("_")
        val speakerId = uttIdComps(2)
        val wavePath = values(1)

        out.write(s"""---
+key: ${uttId}
+speakerId: ${speakerId}
waveform:
  type: ref
  loc: ${wavePath}
  format: audio/x-nist
""")
      }
    }
    output
  }
}


@WeaveFunction class CreateTIMITPhoneCorpus {
  override val version = "2016-02-09v3"

  def makePhoneFSTFromPhnFile(phnPath: Path): String = {
    var st = 0
    val head = "#FST standard\n"
    val body = (for {
      line <- phnPath.lines() ; 
      rawPhn = line.trim.split(raw"\s")(2) ;
      phn = TIMITPhoneMaps.mapFullTo48.getOrElse(rawPhn, rawPhn) ;
      if ! phn.isEmpty
    } yield {
      st += 1
      s"${st-1} ${st} ${phn} ${phn} 0"
    }).mkString("\n")
    val tail = s"\n${st} 0\n"
    head + body + tail
  }

  def apply(fileInfo: p.TextFile): spn.CorpusFile = {
    val output = spn.CorpusFile.output()
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    }{
      for (line <- fileInfo.path.lines()) {
        val values = line.split("\t")
        val uttId = values(0)
        val uttIdComps = uttId.split("_")
        val speakerId = uttIdComps(2)
        val phonePath = Path.fromString(values(2))
        val phoneFST =
          "    "+makePhoneFSTFromPhnFile(phonePath).trim.replace("\n", "\n    ")
        out.write(s"""---
+key: ${uttId}
+speakerId: ${speakerId}
phonegraph:
  type: fst
  data: |
${phoneFST}
""")
      }
    }
    output
  }
}

@WeaveFunction class CreateTIMITInitialAlignment {
  override val version = "2016-02-11v2"
  def makeAlignFST(phnPath: Path, stateFST: String): String = {
    val headPattern = raw"#FSTHeader.*$$".r
    val arcPattern = raw"([0-9]+)\s+([0-9]+)\s+([^\s]+)\s+([^\s]+).*".r
    val finalPattern = raw"([0-9]+)\s+([0-9.]+)".r
    val segs = Seq(0) ++  (for {
      line <- phnPath.lines();
      vals = line.split(raw"\s");
      rawPhn = vals(2);
      phn = TIMITPhoneMaps.mapFullTo48.getOrElse(rawPhn, rawPhn);
      if ! phn.isEmpty()
    } yield {
      val beg = vals(0).toInt
      val end = vals(1).toInt
      val dur = end - beg
      val ts: Seq[Int] = for (i <- 1 to 3) yield {
        val t = beg + (dur * i / 3)
        (t / 16000.0 * 100.0).toInt
      }
      ts
    }).toSeq.flatten

    var curS = 0
    (for (rawLine <- stateFST.split("\n")) yield {
      val line = rawLine.trim
      line match {
        case headPattern() =>
          "#FSTHeader lattice" // rewrite header
        case arcPattern(fromSt, toSt, isym, osym) => {
          if (isym.startsWith("S")) {
            val w = s"(0,0,[${segs(curS)}:${segs(curS+1)}])"
            curS += 1
            s"${fromSt} ${toSt} ${isym} ${osym} ${w}"
          } else {
            val w = s"(0,0,[${segs(curS)}:${segs(curS)}])"
            s"${fromSt} ${toSt} ${isym} ${osym} ${w}"
          }
        }
        case finalPattern(st, w) =>
          s"${st} (0,0,[2147483647:0])"
      }
    }).mkString("\n")
  }
  def apply(stategraph: spn.CorpusFile, fileInfo: p.TextFile)
      : spn.CorpusFile = {
    import scala.collection.JavaConverters._
    import java.util.AbstractMap

    val output = spn.CorpusFile.output()

    val tmpfile = p.TextFile.output()
    val cmd = Seq("spn_corpus_copy",
      "-i", stategraph.path.path, "-o", tmpfile.path.path,
      "--write-text")
    (cmd !!)

    val yaml = new Yaml()
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    }{
      val docs = yaml.loadAll(new java.io.FileInputStream(tmpfile.asFile))
      for {
        (data, infoLine) <- (docs.asScala) zip fileInfo.path.lines().toIterable
      } {
        val infoVals = infoLine.split("\t")
        val phonePath = Path.fromString(infoVals(2))
        val dataMap = data.asInstanceOf[AbstractMap[String, Object]]
        val uttId = dataMap.get("+key").asInstanceOf[String]
        val speakerId = dataMap.get("+speakerId").asInstanceOf[String]
        val stateFST =
          dataMap.get("stategraph").asInstanceOf[AbstractMap[String, Object]]
            .get("data").asInstanceOf[String]
        val alignFST = "    " + makeAlignFST(phonePath, stateFST)
          .trim.replace("\n", "\n    ")
        out.write(s"""---
+key: ${uttId}
+speakerId: ${speakerId}
alignment:
  type: fst
  data: |
${alignFST}
""")

      }
    }

    output
  }
}

case class TIMITCorpus(val rootDir: String, val partition: String,
  val featureExtractor: Plan[spn.CorpusFile]=>Plan[spn.CorpusFile],
  val numSplit: Int) extends AudioCorpus {

  val fileInfo = SplitLinesByModulo(numSplit,
    EnumerateTIMITFiles(rootDir, partition)).unlift(numSplit)

  val audio = fileInfo.map(CreateTIMITWaveCorpus.apply(_))
  override val feature = super.feature
  // ^ This is found to be so important for performance (~20%)
  //   since default implementation generates feature on-the-fly manner

  val phoneGraph = {
    fileInfo.map(CreateTIMITPhoneCorpus.apply(_))
  }

  val wordGraph = phoneGraph
  override val wordGraphTag = "phonegraph"
}

case class TIMITResource(
  val corpus: TIMITCorpus,
  val tree: Plan[spn.TreeFile])
    extends AMTrainingResource {
  val phoneGraph = corpus.wordGraph

  val manualAlign =
    for ((sg, info) <- stateGraph zip corpus.fileInfo) yield {
      CreateTIMITInitialAlignment(sg, info)
    }
}

case class GMMExperiments(
  val resource: TIMITResource,
  val devCorpus: TIMITCorpus,
  val evalCorpus: TIMITCorpus,
  val initTreeStats: Plan[spn.TreeStatsFile],
  val numMix: Int,
  val numFeatures: Int,
  val decodeOpts: Plan[spn.DecodeOpts],
  val gramFST: Plan[fst.FSTFile],
  val nIteMLE: Int = 6
) extends Project {

  val trainMLE = planWith(checkPointPath="train_MLE", trace="MLE Training") {
    TrainGMMByMLE(resource, initTreeStats,
      numMix, numFeatures, nIteMLE)
  }

  val decodeNet = spn.ComposeFST(spn.ComposeFSTOpts(),
    resource.stateContextFST, gramFST)
    .checkPoint("decode_net", level = CheckPointLevel.Result)

  val epochOptimize = PickBestScorer(devCorpus, decodeOpts, decodeNet, 
    trainMLE.allGMMs, WeaveNone, Some(TIMITPhoneMaps.map48To39FST))
  val bestGMM = epochOptimize.bestScorer.checkPoint(
    "bestGMM", level=CheckPointLevel.Result)
  val bestAlign = resource.alignment(bestGMM)
  val lastStats = trainMLE.allStats.last

  val evalScore = 
    ParallelDecoder(
      evalCorpus, decodeOpts, decodeNet, bestGMM, WeaveNone,
      Some(TIMITPhoneMaps.map48To39FST)).score
      .checkPoint("score", level=CheckPointLevel.Result)
}


object TIMIT extends Project {
  val recipeStarted = System.currentTimeMillis

  val rootDir = configVar("TIMIT_ROOT").as[String]
  val extractGMMfeats = {(wave: Plan[spn.CorpusFile]) =>
    spn.FeedFlowWithPreset("mfcc_e_d_a(16000)", wave, "waveform", "feature")}
  val trainCorpus = TIMITCorpus(rootDir, "train", extractGMMfeats, 8)
  val devCorpus = TIMITCorpus(rootDir, "dev", extractGMMfeats, 6)
  val coreCorpus = TIMITCorpus(rootDir, "core", extractGMMfeats, 2)

  // ======================================================================
  // Decoder preparation
  // ======================================================================
  val ngramOrder = 2
  val phoneTexts = for (pg <- trainCorpus.phoneGraph) yield {
    spn.ExtractCorpusText(pg, "phonegraph", false)
  }
  val estimateGramFST =
    EstimateNGramFST(
      ConcatTextFiles(WeaveArray.fromSeq(phoneTexts)),
      ngramOrder)
  val gramFST = estimateGramFST.gramFST
  val phoneList =
    SubstituteRegex(
      raw"^([^\s]+)\s.*$$", "$1",
      FilterByRegex(raw"^<[^>]+>", true, estimateGramFST.vocabulary))
  val numSplit = 12

  // ======================================================================
  // Monophone 
  // ======================================================================

  val monoDecodeOpts = spn.DecodeOpts().copy("acscale" -> BigDecimal("0.2"))
  val monoTree = spn.MakeInitialTree(phoneList, "sil")
  val monoTrainRes = TIMITResource(trainCorpus, monoTree)


  val manualTreeStats =
    trainCorpus.mergedTreeStats(monoTrainRes.manualAlign, 0, 0)

  val monoM1 = planWith(checkPointPath = "mono_M1", trace = "monoGMM M1") {
    GMMExperiments(monoTrainRes, devCorpus, coreCorpus,
      manualTreeStats, 1, 39, monoDecodeOpts, gramFST, numSplit)
  }
  val monoM1TreeStats =
    trainCorpus.mergedTreeStats(monoM1.bestAlign, 0, 0)

  val monoM2 = planWith(checkPointPath = "mono_M2", trace = "monoGMM M2") {
    GMMExperiments(monoTrainRes, devCorpus, coreCorpus,
      monoM1TreeStats, 2, 39, monoDecodeOpts, gramFST, numSplit)
  }
  val monoM4 = planWith(checkPointPath = "mono_M4", trace = "monoGMM M4") {
    GMMExperiments(monoTrainRes, devCorpus, coreCorpus,
      monoM1TreeStats, 4, 39, monoDecodeOpts, gramFST, numSplit)
  }
  val monoM8 = planWith(checkPointPath = "mono_M8", trace = "monoGMM M8") {
    GMMExperiments(monoTrainRes, devCorpus, coreCorpus,
      monoM1TreeStats, 8, 39, monoDecodeOpts, gramFST, numSplit)
  }
  val monoM16 = planWith(checkPointPath = "mono_M16", trace = "monoGMM M16") {
    GMMExperiments(monoTrainRes, devCorpus, coreCorpus,
      monoM1TreeStats, 16, 39, monoDecodeOpts, gramFST, numSplit)
  }

  // ======================================================================
  // Triphone
  // ======================================================================

  val triDecodeOpts = spn.DecodeOpts().copy("acscale" -> BigDecimal("0.1"))
  val triTreeStats = trainCorpus.mergedTreeStats(monoM16.bestAlign, 1, 1)
  val triTree = spn.SplitTree(
    spn.BuildQuestion(monoTrainRes.tree, triTreeStats), triTreeStats, 1000)
    .checkPoint("tritree", level=CheckPointLevel.Result)

  val triTrainRes = TIMITResource(trainCorpus, triTree)
  val triM1 = planWith(checkPointPath = "tri_M1", trace = "triGMM M1") {
    GMMExperiments(triTrainRes, devCorpus, coreCorpus,
      triTreeStats, 1, 39, triDecodeOpts, gramFST, numSplit)
  }
  val triM2 = planWith(checkPointPath = "tri_M2", trace = "triGMM M2") {
    GMMExperiments(triTrainRes, devCorpus, coreCorpus,
      triTreeStats, 2, 39, triDecodeOpts, gramFST, numSplit)
  }
  val triM4 = planWith(checkPointPath = "tri_M4", trace = "triGMM M4") {
    GMMExperiments(triTrainRes, devCorpus, coreCorpus,
      triTreeStats, 4, 39, triDecodeOpts, gramFST, numSplit)
  }
  val triM8 = planWith(checkPointPath = "tri_M8", trace = "triGMM M8") {
    GMMExperiments(triTrainRes, devCorpus, coreCorpus,
      triTreeStats, 8, 39, triDecodeOpts, gramFST, numSplit)
  }
  val triM16 = planWith(checkPointPath = "tri_M16", trace = "triGMM M16") {
    GMMExperiments(triTrainRes, devCorpus, coreCorpus,
      triTreeStats, 16, 39, triDecodeOpts, gramFST, numSplit)
  }

  val gmms = WeaveArray(
    monoM1.bestGMM, monoM2.bestGMM, monoM4.bestGMM, monoM8.bestGMM,
    monoM16.bestGMM,
    triM1.bestGMM, triM2.bestGMM, triM4.bestGMM, triM8.bestGMM,
    triM16.bestGMM)

  val gmmScores = WeaveArray(
    monoM1.evalScore, monoM2.evalScore, monoM4.evalScore, monoM8.evalScore,
    monoM16.evalScore,
    triM1.evalScore, triM2.evalScore, triM4.evalScore, triM8.evalScore,
    triM16.evalScore)

  // ======================================================================
  // DNN
  // ======================================================================
  val aligner = monoM16.bestGMM
  val devStateGraph = for (pg <- devCorpus.phoneGraph) yield {
    spn.ComposeCorpusAndFST(pg, monoTrainRes.stateContextFST,
      monoTrainRes.phoneGraphTag, monoTrainRes.stateGraphTag, false)
  }

  val devAlign = for ((sg,feat) <- (devStateGraph zip devCorpus.feature)) yield {
    spn.Align(monoTrainRes.alignOpts, sg, feat, aligner, WeaveNone)
  }
  val xvalStateId = spn.ConvertAlignToStateIds(
    spn.MergeCorpus(WeaveArray.fromSeq(devAlign)),
    "alignment", "state")

  val trainStateId = spn.ConvertAlignToStateIds(
    monoTrainRes.mergedAlignment(aligner),
    "alignment", "state")

  val (presetName, baseDim) = ("lfbe_e_d_a_z", 120)
  val checkPointSuffix = presetName.toUpperCase()

  val extractRawDNNfeats = {(wave: Plan[spn.CorpusFile]) =>
    spn.FeedFlowWithPreset(s"${presetName}(16000,39)", wave, "waveform", "feature")}
  val trainCorpusDNNRaw = TIMITCorpus(rootDir, "train", extractRawDNNfeats, 1)

  val statsForCMVN = spn.AccumulateCMVNStats(trainCorpusDNNRaw.mergedFeature)
  val whitener = spn.WriteTransformFlow(
    spn.ComputeCMVNTransform(statsForCMVN), "whiten")
    .checkPoint(s"whitener.${checkPointSuffix}")

  val extractDNNfeats = {(wave: Plan[spn.CorpusFile]) =>
    spn.FeedFlow(
      whitener,
      spn.FeedFlowWithPreset(s"${presetName}(16000,39)", wave, "waveform", "feature"),
      "feature", "feature")
  }
  val shuffledIds = spn.ShuffleAndRenameKeyList(
    spn.ListKeysInCorpus(trainCorpus.mergedFeature), 0x5EED)
  val xvalShuffledIds = spn.ShuffleAndRenameKeyList(
    spn.ListKeysInCorpus(devCorpus.mergedFeature), 0x5EED)

  val trainCorpusDNN = TIMITCorpus(rootDir, "train", extractDNNfeats, 1)
  val devCorpusDNN = TIMITCorpus(rootDir, "dev", extractDNNfeats, 6)
  val coreCorpusDNN = TIMITCorpus(rootDir, "core", extractDNNfeats, 2)

  val inputStack = spn.WriteWindowFlow(17, 1, 8, 8, "repeat", "repeat")

  val nInput = baseDim * 17
  val nHidUnit = 2048
  val nOutput = 144
  val nHidLayer = 5
  val nXentIte = 18

  val trainStreams = WeaveArray(
    spn.NNStreamDef("input",
      spn.FilterCorpus(trainCorpusDNN.mergedFeature, shuffledIds),
      "feature", "input", WeaveSome(inputStack)),
    spn.NNStreamDef("xent",
      spn.FilterCorpus(trainStateId, shuffledIds),
      "state", "output", WeaveNone)
  )

  val xvalStreams = WeaveArray(
    spn.NNStreamDef("input",
      spn.FilterCorpus(devCorpusDNN.mergedFeature, xvalShuffledIds),
      "feature", "input", WeaveSome(inputStack)),
    spn.NNStreamDef("xent",
      spn.FilterCorpus(xvalStateId, xvalShuffledIds),
      "state", "output", WeaveNone)
  )

  /*
  val initUpdateOpts = MakeUpdateOpt(
    BigDecimal("0.0001"),
    BigDecimal("0.95"),
    BigDecimal("0.00001")
  )
   */
  val initUpdateOpts = MakeUpdateOpt(
    BigDecimal("0.001"),
    BigDecimal("0"),
    BigDecimal("0")
  )

  val hiddenType = "relu"
  val initNN =
    MakeSimpleDropoutNN(
      nInput, nHidUnit, nHidLayer, nOutput,
      dropoutFraction=BigDecimal("1"),
      randscale=BigDecimal("0.4"),
      hiddenType=hiddenType).builtNN

  val xentTrain =
    planWith(checkPointPath = "mono_DNN_xent", trace = "monoDNN xent") {
      RunXentTrainingWithNewBobScheduling(
        trainStreams, xvalStreams, initNN, nXentIte,
        initUpdateOpts=initUpdateOpts
      )
    }

  val frameErrorPlot = doc.DrawLinePlotPNG(
    doc.AddYValuesToLinePlot(doc.LinePlotData(),
      xentTrain.allFERs, "Frame Error Rates", ""),
    doc.LinePlotFigStyle())
    .checkPoint("mono_DNN_xent/FERs.png",
      level = CheckPointLevel.Result)

  val nnetDecodeOpts = spn.DecodeOpts().copy(
    "acscale" -> (BigDecimal("1.0") / BigDecimal("0.8")))

  val errorRateDNN = ParallelDecoder(coreCorpusDNN, nnetDecodeOpts,
    monoM16.decodeNet, xentTrain.trainedNN, WeaveSome(inputStack),
    Some(TIMITPhoneMaps.map48To39FST)).score
    .checkPoint("mono_DNN_xent/score", level = CheckPointLevel.Result)

  val recipeEvaluated = System.currentTimeMillis
}

