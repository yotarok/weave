package ro.yota.weave.tools.opengrm

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.macrodef._
import scalax.file.Path
import ro.yota.weave.stdlib.ProcessUtils
import sys.process._ 
import ro.yota.weave.tools.openfst.FSTFile

@WeaveRecord class CommonOpts(
  val BOSSymbol: p.String,
  val EOSSymbol: p.String
) {
  def default = {
    new CommonOpts("<s>", "</s>")
  }

  def toArgSeq =
    Seq(
      "--start_symbol=" + BOSSymbol.value,
      "--end_symbol=" + EOSSymbol.value
    )
}


@WeaveRecord class SymbolTableOpts(
  val commonOpts: CommonOpts,
  val OOVSymbol: p.String,
  val epsSymbol: p.String 
) {
  def default = {
    new SymbolTableOpts(CommonOpts.default, "<unk>", "<epsilon>")
  }
 
  def toArgSeq = 
    commonOpts.toArgSeq ++ Seq(
      "--OOV_symbol=" + OOVSymbol.value,
      "--epsilon_symbol=" + epsSymbol.value
    )
}

@WeaveFunction class ExtractSymbols {
  def apply(opts: SymbolTableOpts, text: p.TextFile): p.TextFile = {
    val output = p.TextFile.output();
    val cmd = Seq("ngramsymbols") ++ opts.toArgSeq ++ Seq(text.path.path)
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveRecord class CompileStringsOpts(
  val commonOpts: CommonOpts,
  val arcType: p.String,
  val keepSymbols: p.Boolean,
  val OOVSymbol: p.String
) {
  def default = {
    new CompileStringsOpts(CommonOpts.default, "standard", true, "<unk>")
  }

  def toArgSeq =
    Seq(
      "--arc_type=" + arcType.value,
      "--keep_symbols=" + keepSymbols.value.toString,
      "--unknown_symbol=" + OOVSymbol.value.toString
    )

}

@WeaveFunction class CompileStrings {
  def apply(opts: CompileStringsOpts, symtab: p.TextFile, text: p.TextFile)
      : p.File = {
    val output = p.File.output();
    val cmd = Seq("farcompilestrings") ++ opts.toArgSeq ++ Seq(
      "--symbols=" + symtab.path.path, text.path.path)
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveRecord class CountNGramOpts(
  val commonOpts: CommonOpts,
  val roundToInt: p.Boolean
) {
  def default = {
    new CountNGramOpts(CommonOpts.default, false)
  }

  def toArgSeq =
    commonOpts.toArgSeq ++ Seq(
      "--round_to_int=" + roundToInt.value.toString
    )
}

@WeaveFunction class CountNGram {
  def apply(opts: CountNGramOpts, order: p.Int, far: p.File)
      : FSTFile = {
    val output = FSTFile.output();
    //ngramcount -order=5 earnest.far >earnest.cnts
    val cmd = Seq("ngramcount") ++ opts.toArgSeq ++ Seq(
      "--order=" + order.value.toString, "--method=counts",
      far.path.path)
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveRecord class MakeNGramModelOpts(
  val normEps: p.BigDecimal,
  val wittenBellK: p.BigDecimal
) {
  def default = {
    new MakeNGramModelOpts(p.BigDecimal("0.001"), p.BigDecimal("1"))
  }

  def toArgSeq =
    Seq(
      "--norm_eps=" + normEps.value.toString,
      "--witten_bell_k=" + wittenBellK.value.toString
    )
}


@WeaveFunction class MakeNGramModel {
  def apply(opts: MakeNGramModelOpts, method: p.String, count: FSTFile)
      : FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("ngrammake") ++ opts.toArgSeq ++ Seq(
      "--method=" + method.value, count.path.path)
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveRecord class ComputePerplexityOpts(
  val usePhiMatcher: p.Boolean
) {
  def default = {
    new ComputePerplexityOpts(new p.Boolean(false))
  }

  def toArgSeq =
    Seq(
      "--use_phimatcher=" + usePhiMatcher.value.toString
    )
}

@WeaveFunction class ComputePerplexity {
  def apply(opts: ComputePerplexityOpts, ngram: FSTFile, input: p.File): p.TextFile = {
    val output = p.TextFile.output();
    val cmd = Seq("ngramperplexity") ++ opts.toArgSeq ++ Seq(ngram.path.path, input.path.path)
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}
