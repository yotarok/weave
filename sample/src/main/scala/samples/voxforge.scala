package ro.yota.exp.voxforge

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
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
import org.yaml.snakeyaml._


@WeaveFunction class SplitPrompts {
  def apply(prompts: p.TextFile, numSplit: p.Int)
      : WeaveArray[p.TextFile] = {
    val allTriples = for (line <- prompts.path.lines()) yield {
      line.split(" ", 2).toList match {
        case uttid :: text :: Nil => {
          val archiveId = uttid.split("/")(0)
          (uttid, archiveId, text)
        }
        case _ => throw new RuntimeException("Prompt format error")
      }
    }

    val archiveToContent = allTriples.foldLeft(Map[String, Seq[Tuple2[String, String]]]()) {
      case (map, (uttid, archiveId, text)) => {
        map.get(archiveId) match {
          case Some(oldList) => {
            map + (archiveId -> (oldList ++ Seq(Tuple2(uttid, text))))
          }
          case None => {
            map + (archiveId -> Seq(Tuple2(uttid, text)))
          }
        }
      }
    }

    val splitPrompts = new scala.Array[String](numSplit)
    val uttCounts = new scala.Array[Int](numSplit)
    for (i <- (0 until numSplit)) {
      splitPrompts(i) = ""
      uttCounts(i) = 0
    }
    for ((archiveId, contents) <- archiveToContent) {
      println(s"Archive ${archiveId} has ${contents.size} utterances")
      val minPart = uttCounts.toSeq.zipWithIndex.minBy(_._1)._2
      println(s"To be appended to partition ${minPart}...")
      splitPrompts(minPart) += contents.map({ case (uttid, text) =>
        s"${uttid} ${text}\n"
      }).toSeq.mkString("")
      uttCounts(minPart) += contents.size
    }

    WeaveArray.fromSeq(
      (0 until numSplit).map({i =>
        val out = p.TextFile.output()
        out.path.write(splitPrompts(i))
        out
      })
    )
  }
}

@WeaveFunction class MakeWordCorpusFromPrompt {
  def apply(prompt: p.TextFile): spn.CorpusFile = {
    val output = spn.CorpusFile.output()
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    }{
      for (line <- prompt.path.lines()) {
        val vs = line.split(raw"\s+").toList
        val uttid = vs.head
        val words = vs.tail

        val fstsrc = "#FST standard\n" + words.zipWithIndex.map({case (w, i) =>
            s"${i} ${i+1} ${w} ${w}"
          }).mkString("\n") + s"\n${words.size}"
        
        val fstsrcIndent = fstsrc.replaceAll("(?m)^", "    ")
        out.write(s"""+key: ${uttid}
wordgraph:
  type: fst
  data: |
${fstsrcIndent}
---
""")
      }
    }
    
    output
  }
}

@WeaveFunction class PrepareFeatures {
  def apply(prompt: p.TextFile, baseUrl: p.String): spn.CorpusFile = {
    val workdir = p.Directory.temporary()
    val arcs = (for (line <- prompt.path.lines()) yield {
      val vs = line.split(raw"\s+").toList
      val uttid = vs.head
      uttid.split(raw"/").head + ".tgz"
    }).toSet

    val tarCmd = Seq("tar", "-C", workdir.path.path, "-xzv");
    for (arc <- arcs) {
      println(s"Downloading and extracting ${arc}...")
      try {
        if (baseUrl.startsWith("http://") || baseUrl.startsWith("https://")) {
          (tarCmd #< new java.net.URL(baseUrl + arc)).!!
        } else {
          (tarCmd #< new java.io.File(baseUrl + arc)).!!
        }
      } catch {
        case _: Throwable => {
          println(s"WARNING: Error occurred while extracting ${arc}")
        }
      }
    }

    val textcorpus = p.TextFile.temporary()
    for {
      processor <- textcorpus.path.outputProcessor
      out = processor.asOutput
    }{
      for (line <- prompt.path.lines()) {
        println(s"Making corpus entry for: ${line}")
        val vs = line.split(raw"\s+").toList
        val uttid = vs.head
        val uttidVals = uttid.split(raw"/")
        val arcid = uttidVals(0)
        val sentid = uttidVals.last
        val fnbase = {(ext: String) =>
          s"${workdir.path.path}/${arcid}/${ext}/${sentid}.${ext}"
        }
        val (fn, ctype) = if (Path.fromString(fnbase("wav")).exists) {
          (fnbase("wav"), "audio/wav")
        } else if (Path.fromString(fnbase("flac")).exists) {
          (fnbase("flac"), "audio/flac")
        } else {
          val s = fnbase("*")
          println(s"WARNING: Missing file ${s}")
          //throw new RuntimeException(s"Unknown file type: ${fnbase}")
          (fnbase("*"), "*")
        }
        if (ctype != "*") {
          out.write(s"""+key: ${uttid}
waveform:
  type: ref
  loc: ${fn}
  format: ${ctype}
---
""")
        }
      }
    }

    val output = spn.CorpusFile.output()
    println("Feature extraction from text corpus")
    Seq("spn_flow_feed", "-p", "mfcc_e_d_a(16000)",
      "-i", textcorpus.path.path, "-o", output.path.path,
      "-I", "waveform", "-O", "feature").!!;
    output
  }
}

case class VoxForgeCorpus(
  val audioListBaseUrl: String,
  val promptUrl: String,
  val isTrainSplit: Boolean,
  val numSplit: Int,
  val splitFilter:
      (Seq[Plan[p.TextFile]] => Seq[Plan[p.TextFile]]) = identity
  ) extends Corpus {

  val promptDir = UnarchiveTar(ImportFile(promptUrl), "z")
  val promptFileName = if (isTrainSplit) {
    "Prompts/master_prompts_train_16kHz-16bit"
  } else {
    "Prompts/master_prompts_test_16kHz-16bit"
  }

  val prompt = splitFilter(
    SplitPrompts(
      ExtractFile(promptDir, promptFileName).as[p.TextFile],
      numSplit).unlift(numSplit))
  val textList = prompt.map({
    SubstituteRegex(raw"^[^ ]+ ", "", _)
  })
  val text = ConcatTextFiles(WeaveArray(textList:_*))

  val wordGraph = prompt.map({ text =>
    spn.CopyCorpus(MakeWordCorpusFromPrompt(text), true)
  })

  val feature = prompt.map({ text =>
    PrepareFeatures(text, audioListBaseUrl)
  })

}

object VoxForge extends Project {
  val audioListUrl = "https://voxforge-mirror.s3.eu-central-1.amazonaws.com/www.repository.voxforge1.org/downloads/SpeechCorpus/Trunk/Audio/Main/16kHz_16bit/index.html"
  val audioListBaseUrl = "https://voxforge-mirror.s3.eu-central-1.amazonaws.com/www.repository.voxforge1.org/downloads/SpeechCorpus/Trunk/Audio/Main/16kHz_16bit/"
  val promptUrl = "https://voxforge-mirror.s3.eu-central-1.amazonaws.com/www.repository.voxforge1.org/downloads/SpeechCorpus/Trunk/Prompts/Prompts.tgz"
  val lexiconUrl = "https://voxforge-mirror.s3.eu-central-1.amazonaws.com/www.repository.voxforge1.org/downloads/SpeechCorpus/Trunk/Lexicon/VoxForge.tgz"


  val trainCorpus = VoxForgeCorpus(
    audioListBaseUrl, promptUrl, true, 36,
    _.take(32))
  val devCorpus = VoxForgeCorpus(
    audioListBaseUrl, promptUrl, true, 36,
    _.drop(32))
  val testCorpus = VoxForgeCorpus(audioListBaseUrl, promptUrl, false, 10)

  val makeNGramFST = EstimateNGramFST(trainCorpus.text)

  val nState = 2000

  case class VoxForgeTrainingResource(
    val tree: Plan[spn.TreeFile],
    val lexiconFST: Plan[fst.FSTFile],
    override val disambPhonesStr: Plan[p.String]
  ) extends AMTrainingResourceWithLexicon {
    val corpus = trainCorpus
  }

  val rawLexicon = SubstituteRegex("\\[[^\\]]*\\]", "",
    SubstituteRegex(raw"\([0-9]+\) ", " ",
      ExtractFile(UnarchiveTar(ImportFile(lexiconUrl), "z"),
        "VoxForge/VoxForgeDict").as[p.TextFile]))
  val lexicon = FilterLexicon(rawLexicon, makeNGramFST.vocabulary)
  val makeLexFST = MakeLexiconFST(lexicon,
    Seq("<eps>" -> BigDecimal("0"), "sil" -> BigDecimal("0")))


  val monoTree = spn.MakeInitialTree(makeLexFST.phones, "sil")

  val monoTrainResource = VoxForgeTrainingResource(
    monoTree, makeLexFST.lexFST, makeLexFST.disambPhonesStr)

  val monoM1 = TrainGMMByMLE(
    monoTrainResource,
    trainCorpus.mergedTreeStats(monoTrainResource.equalAlignment, 0, 0),
    1, 39, 12)

  val monoM16 = TrainGMMByMLE(
    monoTrainResource,
    trainCorpus.mergedTreeStats(monoM1.allAligns.last, 0, 0),
    16, 39, 6)

  val baseAlignment = monoM16.allAligns.last

  val triTreeStats =
    trainCorpus.mergedTreeStats(baseAlignment, 1, 1)
  val triTree = spn.SplitTree(
    spn.BuildQuestion(monoTree, triTreeStats), triTreeStats, nState)

  val triTrainResource = VoxForgeTrainingResource(triTree, makeLexFST.lexFST, makeLexFST.disambPhonesStr)

  val stateCtxLexFST = fst.OptimizeFST(
    spn.ComposeFST(spn.ComposeFSTOpts(),
      triTrainResource.stateContextFST, makeLexFST.lexFST))
  val decodeNet = fst.PushWeight(
    spn.ComposeFST(spn.ComposeFSTOpts().copy("lookahead" -> true),
      stateCtxLexFST, makeNGramFST.gramFST))

  val triM16 = TrainGMMByMLE(
    triTrainResource, triTreeStats, 16, 39, 12,
    realignIte = Some(Set(1, 3, 5, 7, 10)))

  val decodeOpts = spn.DecodeOpts().copy("beam" -> BigDecimal("12.0"))
  val epochOptimize = PickBestScorer(
    devCorpus, decodeOpts, decodeNet, triM16.allGMMs, WeaveNone, None)
}
