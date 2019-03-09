package ro.yota.weave.tools.spin

import ro.yota.weave._

import scala.language.implicitConversions
import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.tools.openfst.FSTFile

import ro.yota.weave.macrodef._
import scalax.file.Path
import scalax.io.Output

import org.yaml.snakeyaml._
import sys.process._


@WeaveFileType class CorpusFile(p: Path) extends p.File(p) {
  override def show() {
    val tmpDir = Path.createTempDirectory()
    val prepareProc = Seq("spn_corpus_view", "-o", tmpDir.path, "-i", p.path)
    prepareProc.!
    java.awt.Desktop.getDesktop().browse(
      new java.net.URI("file://" + tmpDir.path + "/index.html"))
    scala.io.StdIn.readLine("Press enter for deleting temporary files")
  }
  override val contentType = "application/octet-stream"
  override val preferredExt = Some(".corpus")
}

@WeaveFileType class ObjectFile(p: Path) extends p.File(p) {
  override def show() {
    ProcessUtils.pipeToPager(Seq("spn_object_copy", "--write-text",
      "-o", "/dev/stdout", "-i", p.path))
  }
}

@WeaveFileType class ScorerFile(p: Path) extends ObjectFile(p) { }
@WeaveFileType class GMMFile(p: Path) extends ScorerFile(p) { }
@WeaveFileType class NNFile(p: Path) extends ScorerFile(p) { }
@WeaveFileType class GMMStatsFile(p: Path) extends ObjectFile(p) { }
@WeaveFileType class TreeFile(p: Path) extends ObjectFile(p) { }
@WeaveFileType class TreeStatsFile(p: Path) extends ObjectFile(p) { }
@WeaveFileType class ErrorStatsFile(p: Path) extends ObjectFile(p) { }
@WeaveFileType class AffineTransFile(p: Path) extends ObjectFile(p) { }


@WeaveFunction class MakeInitialTree {
  def dumpTreeNode(out: Output, ph: p.String, noSplit: p.Boolean,
    nState: Int, stateOffset: Int) {
    val nosp = noSplit.value.toString
    out.write("  - question: {qtype: \"context\", category: \""+ph.value+"\", context: 0}\n")
    out.write("    isTrue:\n")
    for (s <- 1 to (nState - 1)) {
      out.write("      - question: {qtype: \"location\", value: "+(s-1)+"}\n")
      out.write("        isTrue: {leaf: "+(stateOffset + s - 1)+", nosplit: "+(nosp)+"}\n")
    }
    out.write("        isFalse: {leaf: "+(stateOffset + nState - 1)+", nosplit: "+(nosp)+"}\n")
  }

  def apply(phonelist: p.TextFile, silphonelist: p.String): TreeFile = {
    val output = TreeFile.output()
    val silphones = silphonelist.trim.split(",")
    val phones = phonelist.path.lines().map(_.trim)
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    } {
      out.write("categories:\n")
      for (p <- phones) {
        out.write("  \"" + p + "\": [\"" + p + "\"]\n")
      }
      out.write("contextLengths: [0, 0]\n")
      out.write("root:\n")

      var stateoff = 0
      for (ph <- silphones) {
        dumpTreeNode(out, ph, true, 3, stateoff)
        stateoff += 3
      }
      for {
        ph <- phones;
        if (! (silphones contains ph))
          } {
        dumpTreeNode(out, ph, false, 3, stateoff)
        stateoff += 3
      }
    }
    output
  }
}

@WeaveFunction class ConvertTreeToHCFST  {

  def apply(tree: TreeFile, disamb: p.String): FSTFile = {
    val output = FSTFile.output()
    ProcessUtils.execute(
      Seq("spn_tree_to_hcfst",
        "-t", tree.path.path, "-o", output.path.path, "--disamb", disamb.value))

    output
  }
}


@WeaveRecord class ComposeFSTOpts(
  val lookahead: p.Boolean
) {
  def default = {
    new ComposeFSTOpts(false)
  }

  def toArgSeq =
    (if (lookahead) { Seq("--lookahead") } else { Seq() })
}


@WeaveFunction class ComposeFST {
  def apply(opts: ComposeFSTOpts,
    left: FSTFile, right: FSTFile): FSTFile = {
    val output = FSTFile.output()
    val cmd = Seq("spn_fst_compose") ++ opts.toArgSeq ++ Seq(
      "-l", left.path.path, "-r", right.path.path, "-o", output.path.path)
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class CopyCorpus {
  def apply(src: CorpusFile, binary: p.Boolean): CorpusFile = {
    val output = CorpusFile.output()
    val writeopt = if (binary) {
      Seq()
    } else {
      Seq("--write-text")
    }
    val cmd = Seq("spn_corpus_copy") ++ writeopt ++
      Seq("-i", src.path.path, "-o", output.path.path)
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class ListKeysInCorpus {
  def apply(input: CorpusFile): p.TextFile = {
    val output = p.TextFile.output()
    val cmd = Seq("spn_corpus_list",
      "-i", input.path.path, "-o", output.path.path)
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class ShuffleAndRenameKeyList {
  def apply(input: p.TextFile, seed: p.Int): p.TextFile = {
    val output = p.TextFile.output()
    val oids = (for (l <- input.path.lines()) yield {
      l.trim
    }).toSeq

    val idxs = (new scala.util.Random(seed.value)).shuffle(0 to (oids.size - 1))
    val ndigit = (Math.log10(oids.size) + 1.0).toInt
    val idxFormat = "%0" + ndigit.toString + "d"

    val renameFilter = (for ((idx, oid) <- idxs zip oids) yield {
      idxFormat.format(idx) + "_" + oid + "\t" + oid
    }).sorted

    //println(s"${renameFilter}")

    output.path.writeStrings(renameFilter, "\n")
    output
  }
}

@WeaveFunction class FilterCorpus {
  def apply(input: CorpusFile, filter: p.TextFile): CorpusFile = {
    val output = CorpusFile.output()
    val cmd = Seq("spn_corpus_filter",
      "-s", input.path.path, "-o", output.path.path,
      "-f", filter.path.path)
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class ProjectCorpusFST {
  def apply(src: CorpusFile, projOut: p.Boolean, rmEps: p.Boolean,
    intag: p.String, outtag:p.String)
      : CorpusFile = {
    val output = CorpusFile.output()
    val projTypeArg = if (projOut) { Seq("--proj-out") } else { Seq() }
    val rmEpsArg = if (rmEps) { Seq("--rmeps") } else { Seq() }
    val cmd = Seq("spn_corpus_fst_project") ++ projTypeArg ++ rmEpsArg ++ Seq(
      "-i", src.path.path, "-o", output.path.path,
      "-I", intag.value, "-O", outtag.value)
    ProcessUtils.execute(cmd)
    output
  }
}


@WeaveFunction class ComposeCorpusAndFST {

  def apply(corpus: CorpusFile, fst: FSTFile,
    intag: p.String, outtag: p.String,
    rightCompose: p.Boolean)
      : CorpusFile = {
    val output = CorpusFile.output()
    val cmd = Seq("spn_corpus_fst_compose", "-i", corpus.path.path,
      "-o", output.path.path, "-f", fst.path.path,
      "-I", intag.value, "-O", outtag.value) ++ (if (rightCompose) {
        Seq("--rightcompose")
      } else { Seq() }
    )
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class ComputeEqualAlignment {
  def apply(stategraph: CorpusFile, feature: CorpusFile) : CorpusFile = {
    val output = CorpusFile.output()
    ProcessUtils.execute(Seq("spn_equal_align", "-s", stategraph.path.path,
      "-f", feature.path.path, "-o", output.path.path))
    output
  }
}

@WeaveFunction class AccumulateGMMStats {
  def apply(alignment: CorpusFile, feature: CorpusFile,
    curgmm: GMMFile)
      : GMMStatsFile = {
    val output = GMMStatsFile.output()
    ProcessUtils.execute(Seq("spn_gmm_acc", "-a", alignment.path.path,
      "-f", feature.path.path,
      "-g", curgmm.path.path,
      "-o", output.path.path))
    output
  }
}

@WeaveFunction class MergeGMMStats {
  def apply(stats: WeaveSeq[GMMStatsFile]): GMMStatsFile = {
    val output = GMMStatsFile.output()
    val cmd = Seq("spn_gmm_acc_merge", "-o", output.path.path) ++
      stats.value.map { file =>
        file.path.path
      }
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveRecord class UpdateGMMOpts(
  val varMinOcc: p.BigDecimal,
  val varFloor: p.BigDecimal
) {
  def default = {
    new UpdateGMMOpts(p.BigDecimal("0.001"), p.BigDecimal("0.001"))
  }
  def toArgSeq =
    Seq(
      "--var-minocc", this.varMinOcc.value.toString(),
      "--var-floor", this.varFloor.value.toString()
    )
}

@WeaveFunction class UpdateGMM {
  def apply(opts: UpdateGMMOpts, curgmm: GMMFile, stats: GMMStatsFile)
      : GMMFile = {
    val output = GMMFile.output()
    val cmd = Seq("spn_gmm_est", "-i", curgmm.path.path,
      "-s", stats.path.path,
      "-o", output.path.path) ++ opts.toArgSeq
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class AccumulateTreeStats {
  def apply(alignment: CorpusFile, features: CorpusFile,
    left: p.Int, right: p.Int)
      : TreeStatsFile = {
    val output = TreeStatsFile.output()
    ProcessUtils.execute(Seq("spn_tree_acc", "-a", alignment.path.path,
      "-f", features.path.path, "-o", output.path.path,
      "-l", left.toString, "-r", right.toString))
    
    output
  }
}


@WeaveFunction class MergeTreeStats {
  def apply(stats: WeaveSeq[TreeStatsFile]): TreeStatsFile = {
    val output = TreeStatsFile.output()
    val cmd = Seq("spn_tree_acc_merge", "-o", output.path.path) ++
      stats.value.map { file =>
        file.path.path
      }
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class InitGMM {
  def apply(tree: TreeFile, stats: TreeStatsFile, nMix: p.Int, nDim: p.Int)
      : GMMFile = {
    val output = GMMFile.output()
    ProcessUtils.execute(Seq("spn_gmm_init",
      "-m", nMix.toString, "-d", nDim.toString,
      "--treestat", stats.path.path,
      "-t", tree.path.path, "-o", output.path.path))
    output
  }

}

@WeaveRecord class AlignOpts (
  val beam: p.BigDecimal,
  val maxactive: p.Int,
  val acscale: p.BigDecimal
) {
  def default = {
    new AlignOpts(p.BigDecimal("200.0"), 30000, p.BigDecimal("0.1"))
  }
  def toArgSeq =
    Seq(
      "--beam", this.beam.value.toString(),
      "--maxactive", this.maxactive.value.toString(),
      "--acscale", this.acscale.value.toString()
    )

}

@WeaveFunction class Align {
  def apply(opts: AlignOpts,
    stategraphs: CorpusFile, features: CorpusFile, gmm: ScorerFile,
    flow: WeaveOption[p.File])
      : CorpusFile = {
    val output = CorpusFile.output()
    val flowOpts = if (flow.value.isEmpty) {
      Seq()
    } else {
      Seq("--flow", flow.value.get.path)
    }
    val cmd = Seq("spn_align",
      "-s", stategraphs.path.path,
      "-f", features.path.path,
      "-m", gmm.path.path,
      "-o", output.path.path) ++ opts.toArgSeq ++ flowOpts
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class BuildQuestion {
  def apply(tree: TreeFile, treestats: TreeStatsFile)
      : TreeFile = {
    val output = TreeFile.output()

    ProcessUtils.execute(Seq("spn_tree_build_question",
      "-i", tree.path.path,
      "-s", treestats.path.path,
      "-o", output.path.path,
      "--write-text")
    )

    output
  }
}

@WeaveFunction class SplitTree {

  def apply(tree: TreeFile, treestats: TreeStatsFile, maxsplit: p.Int)
      : TreeFile = {
    val output = TreeFile.output()

    ProcessUtils.execute(Seq("spn_tree_split",
      "-i", tree.path.path,
      "-s", treestats.path.path,
      "-o", output.path.path,
      "--maxstate", maxsplit.toString,
      "--write-text")
    )

    output
  }
}

 

@WeaveFunction class ConvertLatticeToGraph {
  def apply(input: CorpusFile, inputtag: p.String, outputtag: p.String)
      : CorpusFile = {
    val output = CorpusFile.output()
    ProcessUtils.execute(Seq("spn_corpus_fst_project",
      "-i", input.path.path,
      "--inputtag", inputtag.value,
      "-o", output.path.path,
      "--outputtag", outputtag.value,
      "--proj-out", "--to-graph", "--rmeps"))
    output
  }
}

@WeaveRecord class DecodeOpts(
  val beam: p.BigDecimal,
  val maxactive: p.Int,
  val acscale: p.BigDecimal
) {
  def default = {
    new DecodeOpts(p.BigDecimal("20.0"), 8000, p.BigDecimal("0.1"))
  }

  def toArgSeq =
    Seq(
      "--beam", this.beam.value.toString(),
      "--maxactive", this.maxactive.value.toString(),
      "--acscale", this.acscale.value.toString()
    ) 


}

@WeaveFunction class Decode {
  def apply(opts: DecodeOpts, feats: CorpusFile, scorer: ScorerFile,
    flow: WeaveOption[p.File], net: FSTFile): CorpusFile = {
    val output = CorpusFile.output()
    val flowOpts = if (flow.value.isEmpty) {
      Seq()
    } else {
      Seq("--flow", flow.value.get.path)
    }
    val cmd = Seq("spn_decode",
      "-g", net.path.path,
      "-f", feats.path.path,
      "-m", scorer.path.path,
      "-o", output.path.path) ++ opts.toArgSeq ++ flowOpts
    ProcessUtils.execute(cmd)
    output    
  }
}

@WeaveFunction class AlignHypothesis {
  override val version = "2016-02-11"
  def apply(hypo: CorpusFile, hypotag: p.String,
    ref: CorpusFile, reftag: p.String):
      p.Tuple2[ErrorStatsFile, CorpusFile] = {
    val stat = ErrorStatsFile.output()
    val align = CorpusFile.output()

    ProcessUtils.execute(Seq("spn_corpus_fst_align",
      "--ref", ref.path.path,
      "--reftag", reftag.value,
      "--hyp", hypo.path.path,
      "--hyptag", hypotag.value,
      "--output", align.path.path,
      "--stat-output", stat.path.path
    ))
    (stat, align)
  }
}

@WeaveFunction class ExtractWER {
  def apply(score: ErrorStatsFile): p.BigDecimal = {
    import java.util.AbstractMap

    val textstat = p.TextFile.temporary()
    ProcessUtils.execute(Seq("spn_object_copy",
      "-i", score.path.path, "-o", textstat.path.path, "--write-text"))

    val yaml = new Yaml()
    val map = yaml.load(textstat.path.string).asInstanceOf[AbstractMap[String, Object]]
    val errCount = (
      map.get("token_sub_err").asInstanceOf[Int] +
        map.get("token_del_err").asInstanceOf[Int] +
        map.get("token_ins_err").asInstanceOf[Int]).asInstanceOf[Double]
    val wer = errCount / map.get("token_total").asInstanceOf[Int]
    p.BigDecimal(wer)
  }
}


@WeaveFunction class ExtractCorpusText {
  def apply(input: CorpusFile, fstTag: p.String, useInput: p.Boolean):
      p.TextFile = {
    import scala.collection.JavaConversions._
    import scala.collection.immutable.StringOps
    import java.util.AbstractMap
    val output = p.TextFile.output()

    val textcorpus = p.TextFile.temporary()
    ProcessUtils.execute(Seq("spn_corpus_copy",
      "-i", input.path.path, "-o", textcorpus.path.path, "--write-text"))

    val yaml = new Yaml()
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    }{
      for (data <- yaml.loadAll(new java.io.FileInputStream(textcorpus.asFile))) {
        println("Loading YAML")
        val fstsrc = data.asInstanceOf[AbstractMap[String, Object]].
          get(fstTag.value).asInstanceOf[AbstractMap[String, Object]].
          get("data").asInstanceOf[String]

        val text = new StringOps(fstsrc).lines.map { l =>
          if (l.startsWith("#")) {
            None
          } else {
            val cs = l.split(raw"\s")
            if (cs.length < 4) {
              None
            } else {
              val idx = if (useInput.value) { 2 } else { 3 }
              Some(cs(idx).trim)
            }
          }
        }.filterNot(_ isEmpty).map(_ get).mkString(" ")
        out.write(text + "\n")
      }
    }
    output
  }
}

@WeaveFunction class AccumulateCMVNStats {
  def apply(feature: CorpusFile)
      : GMMStatsFile = {
    val output = GMMStatsFile.output()
    ProcessUtils.execute(Seq("spn_afftr_cmvn_acc", 
      "-f", feature.path.path,
      "-o", output.path.path))
    output
  }
}

@WeaveFunction class ComputeCMVNTransform {
  def apply(gmmStats: GMMStatsFile):
      AffineTransFile = {
    val output = AffineTransFile.output()

    ProcessUtils.execute(Seq("spn_afftr_cmvn",
      "--stats", gmmStats.path.path,
      "--output", output.path.path
    ))
    output
  }
}

@WeaveFunction class WriteTransformFlow {
  def apply(trans: AffineTransFile, nodeName: p.String): p.TextFile = {
    val output = p.TextFile.output()

    ProcessUtils.execute(Seq("spn_afftr_write_flow",
      "--input", trans.path.path,
      "--output", output.path.path,
      "--nodename", nodeName.value))
    output
  }
}

@WeaveFunction class WriteWindowFlow {
  def apply(size: p.Int, shift: p.Int, leftPad: p.Int, rightPad: p.Int,
    leftPadType: p.String, rightPadType: p.String): p.TextFile = {
    val output = p.TextFile.output()

    val source = s"""
node:
  - name: window
    type: window
    shift: ${shift.value}
    size: ${size.value}
    leftpad: {size: ${leftPad.value}, type: ${leftPadType.value}}
    rightpad: {size: ${rightPad.value}, type: ${rightPadType.value}}
connection:
  - { from: _input, to: window }
  - { from: window, to: _output }
""".trim
    output.path.write(source)
    output
  }
}

@WeaveFunction class WriteNoisyWindowFlow {
  override val version="20151113"
  def apply(size: p.Int, shift: p.Int, leftPad: p.Int, rightPad: p.Int,
    leftPadType: p.String, rightPadType: p.String, noiseStdev: p.BigDecimal)
      : p.TextFile = {
    val output = p.TextFile.output()

    val source = s"""
node:
  - name: window
    type: window
    shift: 1
    size: ${size.value}
    leftpad: {size: ${leftPad.value}, type: ${leftPadType.value}}
    rightpad: {size: ${rightPad.value}, type: ${rightPadType.value}}
  - name: noise
    type: Gaussian_noise
    stdev: ${noiseStdev.value.toString}
connection:
  - { from: _input, to: window }
  - { from: window, to: noise }
  - { from: noise, to: _output }
""".trim
    output.path.write(source)
    output
  }
}

@WeaveFunction class FeedFlow {
  def apply(flow: p.TextFile, input: CorpusFile,
    inputtag: p.String, outputtag: p.String): CorpusFile = {
    val output = CorpusFile.output()

    ProcessUtils.execute(Seq("spn_flow_feed",
      "--input", input.path.path,
      "--output", output.path.path,
      "--flow", flow.path.path,
      "--outputtag", outputtag.value,
      "--inputtag", inputtag.value))
    output
  }
}

@WeaveFunction class FeedFlowWithPreset {
  override val version="2016-02-13v2"
  def apply(presetName: p.String, input: CorpusFile,
    inputtag: p.String, outputtag: p.String): CorpusFile = {
    val output = CorpusFile.output()
    try {
      ProcessUtils.execute(Seq("spn_flow_feed",
        "--input", input.path.path,
        "--output", output.path.path,
        "--preset", presetName.value,
        "--outputtag", outputtag.value,
        "--inputtag", inputtag.value))
    } catch {
      case e: java.lang.RuntimeException => {
        println(s"Exception caught but ignored: ${e}")
      }
    }
    output
  }
}

@WeaveFunction class ConvertAlignToStateIds {
  def apply(input: CorpusFile, inputtag: p.String, outputtag: p.String)
      : CorpusFile = {
    val output = CorpusFile.output()

    ProcessUtils.execute(Seq("spn_align_to_stid",
      "--input", input.path.path,
      "--inputtag", inputtag.value,
      "--output", output.path.path,
      "--outputtag", outputtag.value
    ))
    output

  }
}

@WeaveFunction class CreateEmptyNN {
  def apply(): NNFile = {
    val output = NNFile.output()

    ProcessUtils.execute(Seq("spn_nnet_empty",
      "--output", output.path.path
    ))
    output
  }
}

@WeaveFunction class AddNodeToNN {
  def apply(
    input: NNFile, ntype: p.String, nname: p.String,
    prevs: WeaveSeq[p.String],
    initopts: WeaveSeq[p.Tuple2[p.String, p.String]]) : NNFile = {

    val output = NNFile.output()

    val cmd = Seq("spn_nnet_add_node",
      "--output", output.path.path,
      "--input", input.path.path,
      "--type", ntype.value,
      "--name", nname.value) ++ (
      prevs.value.map { s => {
        Seq("--prev", s.value) }}).flatten ++ (
      initopts.value.map { t => {
        Seq("--init", t.value._1 + "=" + t.value._2) }}).flatten
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveRecord class NNMiniBatchOpts(
  val batchsize: p.Int,
  val cachesize: p.Int,
  val seed: p.Int
) {
  def default = {
    new NNMiniBatchOpts(1024, 262114, 0x5EED)
  }

  def toArgSeq =
    Seq(
      "--seed", this.seed.value.toString(),
      "--batchsize", this.batchsize.value.toString(),
      "--cachesize", this.cachesize.value.toString()
    )
}

@WeaveRecord class NNStreamDef(
  val streamType: p.String,
  val source: CorpusFile,
  val tagName: p.String,
  val nodeName: p.String,
  val preprocessor: WeaveOption[p.File]
) {
  def default = {
    throw new RuntimeException("NNStreamDef doesn't have default constructor");
  }
  def toArgString: String = {
    val prep = if (preprocessor.value.isEmpty) { "" } else {
      ":" + preprocessor.value.get.path
    }
    streamType.value + ":" + source.path.path + ":" + tagName.value +
    ":" + nodeName.value + prep
  }
}

@WeaveRecord class NNUpdateOpts(
  val pairs: WeaveSeq[p.Tuple2[p.String, p.String]]
) {
  def default = {
    new NNUpdateOpts(new WeaveArray[p.Tuple2[p.String, p.String]](IndexedSeq()))
  }

  def update(k: String, v: String): NNUpdateOpts = {
    val pair = p.Tuple2(new p.String(k), new p.String(v))
    new NNUpdateOpts(
      WeaveArray.fromSeq(pairs.value :+ pair)
    )
  }

  def toArgSeq = 
    (pairs.value.map { t => {
      Seq("--updateparam", t.value._1 + "=" + t.value._2)
    }}).flatten
}

@WeaveFunction class SetLearningRate {
  def apply(opt: NNUpdateOpts, target: p.String, v: p.BigDecimal): NNUpdateOpts =
    opt.update("learnrate@" + target.value, v.toString)
}

@WeaveFunction class GetMaximumLearningRate {
  def apply(opt: NNUpdateOpts): p.BigDecimal = {
    val d = (for {
      pair <- opt.pairs.value ;
      if pair.value._1.value.startsWith("learnrate@")
    } yield {
      BigDecimal(pair.value._2.value)
    }).max
    p.BigDecimal(d)
  }
}

@WeaveFunction class SetMomentum {
  def apply(opt: NNUpdateOpts, target: p.String, v: p.BigDecimal): NNUpdateOpts =
    opt.update("momentum@" + target.value, v.toString)
}

@WeaveFunction class SetL2Regularization {
  def apply(opt: NNUpdateOpts, target: p.String, v: p.BigDecimal): NNUpdateOpts =
    opt.update("l2reg@" + target.value, v.toString)
}

@WeaveFunction class SetBiasL2Regularization {
  def apply(opt: NNUpdateOpts, target: p.String, v: p.BigDecimal): NNUpdateOpts =
    opt.update("bias_l2reg@" + target.value, v.toString)
}

@WeaveFunction class SetMaxNorm {
  def apply(opt: NNUpdateOpts, target: p.String, v: p.BigDecimal): NNUpdateOpts =
    opt.update("maxnorm@" + target.value, v.toString)
}

@WeaveFunction class ExtractNNFrameErrorRate {
  def apply(streamstat: ObjectFile, targetTag: p.String): p.BigDecimal = {
    import java.util.AbstractMap

    val yaml = new Yaml()
    val map = yaml.load(streamstat.path.string)
      .asInstanceOf[AbstractMap[String, Object]]
    val stream = map.get(targetTag.value).asInstanceOf[AbstractMap[String, Object]]
    val nErr = stream.get("nerror").asInstanceOf[Int]
    val nFrame = stream.get("nframes").asInstanceOf[Int]
    p.BigDecimal(scala.BigDecimal(nErr) / scala.BigDecimal(nFrame))
  }
}


@WeaveFunction class CancelXEntBias {
  def apply(input: NNFile,
    tree: TreeFile, treestat: TreeStatsFile, target: p.String)
      : NNFile = {
    val output = NNFile.output()
    ProcessUtils.execute(Seq("spn_nnet_cancel_bias",
        "-i", input.path.path,
        "-o", output.path.path,
        "-t", tree.path.path,
        "-s", treestat.path.path,
        "-c", target.value
      ))
    output
  }
}

@WeaveFunction class TrainNNByShuffledSGD {
  def apply(
    input: NNFile, 
    batchopts: NNMiniBatchOpts, streams: WeaveSeq[NNStreamDef],
    updateopts: NNUpdateOpts): NNFile = {
    val output = NNFile.output()

    val streamargs = (streams.value.map { s => {
      Seq("--streamspec", s.toArgString)
    }}).flatten

    val cmd =
      Seq("spn_nnet_shuffle_sgd",
        "-i", input.path.path,
        "-o", output.path.path) ++
        batchopts.toArgSeq ++
        updateopts.toArgSeq ++ streamargs
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class UpdateLearningRateWithNewBob {
  def apply(curUpdateOpts: NNUpdateOpts,
    prevscore: p.BigDecimal, curscore: p.BigDecimal, minimize: p.Boolean,
    factor: p.BigDecimal, threshold: p.BigDecimal)
      : NNUpdateOpts = {
    val penalize = if (minimize.value) {
      (prevscore.value * threshold.value < curscore.value)
    } else {
      (prevscore.value * threshold.value > curscore.value)
    }
    println(
      s"Compared ${prevscore.value} * ${threshold.value} vs ${curscore.value}")
    if (penalize) {
      println(s"And, decided to penalize learning rates by ${factor.value}")
      val npair = for (pair <- curUpdateOpts.pairs.value) yield {
        val nv = if (pair._1.value.startsWith("learnrate")) {
          new p.String((BigDecimal(pair._2.value) * factor.value).toString)
        } else {
          pair._2
        }
        new p.Tuple2(pair._1, nv)
      }
      new NNUpdateOpts(WeaveArray.fromSeq(npair))
    } else {
      println(s"And, decided not to penalize learning rate")
      curUpdateOpts
    }
  }
}

@WeaveFunction class RemoveNodeFromNN {
  def apply(
    input: NNFile, names: WeaveSeq[p.String]) : NNFile = {

    val output = NNFile.output()

    val cmd = Seq("spn_nnet_del_node",
      "--output", output.path.path,
      "--input", input.path.path
    ) ++ names.value.map(_.value)
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class EvaluateNN {
  def apply(input: NNFile, streams: WeaveSeq[NNStreamDef]): ObjectFile = {
    val output = ObjectFile.output()

    val streamargs = (streams.value.map { s => {
      Seq("--streamspec", s.toArgString)
    }}).flatten

    val cmd =
      Seq("spn_nnet_eval",
        "--write-text",
        "-i", input.path.path,
        "-o", output.path.path) ++ streamargs
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class MergeScore {
  def apply(input: WeaveSeq[ErrorStatsFile]): ErrorStatsFile = {
    val output = ErrorStatsFile.output()
    val cmd = (Seq("spn_score_merge", "--write-text", "-o", output.path.path) ++
      input.value.map { file =>
        file.path.path
      })
    ProcessUtils.execute(cmd)
    output
  }
}

@WeaveFunction class MergeCorpus {
  def apply(corpora: WeaveSeq[CorpusFile]): CorpusFile = {
    val output = CorpusFile.output()
    val cmd = (Seq("spn_corpus_merge", "-o", output.path.path) ++
      corpora.value.map { file =>
        file.path.path
      })
    ProcessUtils.execute(cmd)
    output
  }
}
