package ro.yota.weave.tools.openfst

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib.{ProcessUtils,WeaveSeq}

import ro.yota.weave.macrodef._
import scalax.file.Path
import sys.process._
import scala.util.matching._

@WeaveFileType class FSTFile(path: Path) extends p.File(path) {
  override def show() {
    val tmpPath = Path.createTempFile()
    val tmpPathFile = new java.io.File(tmpPath.path);
    ProcessUtils.pipeToPager(Seq("fstinfo", path.path))
    ProcessUtils.pipeToPager(Seq("fstprint", path.path))
  }

  override val preferredExt = Some(".fst")

  def extractInfo(): Map[String, String] = {
    val info = p.TextFile.temporary()
    ProcessUtils.execute(Seq("fstinfo", path.path) #> info.asFile)
    val key_val = raw"^(.*)\s+([^ ]+)$$".r
    (for (l <- info.path.lines()) yield {
      l match {
        case key_val(k, v) => {
          Some((k.trim, v.trim))
        }
        case _ => {
          None
        }
      }
    }).flatten.toMap
  }
  override def isValid(): Boolean = {
    val info = this.extractInfo()
    (info.getOrElse("initial state", "-1") != "-1" &&
      info.getOrElse("# of final states", "0") != "0" &&
      info.getOrElse("# of states", "0") != "0" &&
      info.getOrElse("# of arcs", "0") != "0")
  }

}

object ExtractSymbolsImpl {
  def apply(src: p.TextFile, assigned: WeaveSeq[p.String], col: Int)
      : p.TextFile = {
    val output = p.TextFile.output()
    val set = src.path.lines().map { line =>
      val vs = line.split(raw"\s+")
      if (vs.size > col) {
        Some(vs(col))
      } else {
        None
      }
    }.filterNot(_.isEmpty).map(_.get).toSet
    val assignedSet = assigned.value.map(_.value).toSet
    for {
      processor <- output.path.outputProcessor
      out = processor.asOutput
    } {
      for ((ps, idx) <- assigned.value.zipWithIndex) {
        out.write(s"${ps.value}\t${idx}\n")
      }
      var idx = assigned.value.size
      for {
        s <- set.toSeq.sorted ;
        if ! assignedSet.contains(s)
      } {
        out.write(s"${s}\t${idx}\n")
        idx += 1
      }
    }
    output
  }
}

@WeaveFunction class ExtractInputSymbolsFromSource {
  def apply(src: p.TextFile, assigned: WeaveSeq[p.String]): p.TextFile =
    ExtractSymbolsImpl(src, assigned, 2)
}

@WeaveFunction class ExtractOutputSymbolsFromSource {
  def apply(src: p.TextFile, assigned: WeaveSeq[p.String]): p.TextFile =
    ExtractSymbolsImpl(src, assigned, 3)
}

@WeaveFunction class ExtractInputSymbols {
  def apply(src: FSTFile): p.TextFile = {
    val output = p.TextFile.output();
    ProcessUtils.execute(Seq("fstprint", "--save_isymbols=" + output.path.path,
      src.path.path, "/dev/null"))
    output
  }
}

@WeaveFunction class ExtractOutputSymbols {
  def apply(src: FSTFile): p.TextFile = {
    val output = p.TextFile.output();
    ProcessUtils.execute(Seq("fstprint", "--save_osymbols=" + output.path.path,
      src.path.path, "/dev/null"))
    output
  }
}

@WeaveFunction class RelabelInputWithNumberPairs {
  def apply(src: FSTFile, pairs: p.TextFile): FSTFile = {
    val output = FSTFile.output();
    ProcessUtils.execute(Seq("fstrelabel", "--relabel_ipairs=" + pairs.path.path,
      src.path.path, output.path.path))
    output
  }
}

@WeaveFunction class RelabelOutputWithNumberPairs {
  def apply(src: FSTFile, pairs: p.TextFile): FSTFile = {
    val output = FSTFile.output();
    ProcessUtils.execute(Seq("fstrelabel", "--relabel_opairs=" + pairs.path.path,
      src.path.path, output.path.path))
    output
  }
}


@WeaveRecord class CompileFSTOpts(
  val arcType: p.String,
  val fstType: p.String,
  val keepISymbols: p.Boolean,
  val keepOSymbols: p.Boolean
) {
  def default =
    new CompileFSTOpts("standard", "vector", false, false)
 
  def toArgSeq = 
    Seq(
      "--arc_type=" + arcType.value,
      "--fst_type=" + fstType.value,
      "--keep_isymbols=" + keepISymbols.value.toString,
      "--keep_osymbols=" + keepOSymbols.value.toString
    )
}

@WeaveFunction class CompileFST {
  def apply(opts: CompileFSTOpts, src: p.TextFile,
    isymtab: p.TextFile, osymtab: p.TextFile)
      : FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("fstcompile") ++ opts.toArgSeq ++ Seq(
      "--isymbols=" + isymtab.path.path,
      "--osymbols=" + osymtab.path.path,
      src.path.path);
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveFunction class DeterminizeFST {
  def apply(fst: FSTFile): FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("fstdeterminize", fst.path.path);
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveFunction class OptimizeFST {
  def apply(fst: FSTFile): FSTFile = {
    val output = FSTFile.output();
    val cmd1 = Seq("fstdeterminize", fst.path.path);
    val cmd2 = Seq("fstminimize");
    ProcessUtils.execute(cmd1 #| cmd2 #> output.asFile)
    output
  }
}

@WeaveRecord class ComposeFSTOpts(
  val filter: p.String,
  val connect: p.Boolean
) {
  def default =
    new ComposeFSTOpts("auto", true)

  def toArgSeq = 
    Seq(
      "--compose_filter=" + filter.value,
      "--connect=" + connect.value.toString
    )
}

@WeaveFunction class ComposeFST {
  def apply(opts: ComposeFSTOpts, leftfst: FSTFile, rightfst: FSTFile)
      : FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("fstcompose") ++ opts.toArgSeq ++ Seq(
      leftfst.path.path, rightfst.path.path)
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveFunction class ConcatFST {
  def apply(leftfst: FSTFile, rightfst: FSTFile)
      : FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("fstconcat",
      leftfst.path.path, rightfst.path.path);
    ProcessUtils.execute(cmd #> output.asFile);
    output
  }
}

@WeaveFunction class GetClosureFST {
  def apply(fst: FSTFile, usePlus: p.Boolean)
      : FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("fstclosure", "--closure_plus=" + usePlus.value.toString,
      fst.path.path);
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}

@WeaveFunction class PushWeight {
  def apply(fst: FSTFile) : FSTFile = {
    val output = FSTFile.output();
    val cmd = Seq("fstpush", "--push_weights=true",
      fst.path.path);
    ProcessUtils.execute(cmd #> output.asFile)
    output
  }
}
