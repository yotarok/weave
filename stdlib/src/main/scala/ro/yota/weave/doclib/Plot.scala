package ro.yota.weave.doclib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.macrodef._

import sys.process._
import ro.yota.weave.stdlib.ProcessUtils
import scalax.file.Path

@WeaveFileType class ImageFile(p: Path) extends p.File(p) {
  override def show() {
    val (defaultViewer, needWait) = System.getProperty("os.name") match {
      case "Mac OS X" => (Seq("open", "-a", "Preview"), true)
      case _ => (Seq("display"), false)
    }
    val viewerBin =
      sys.env.get("IMAGE_VIEWER").map(Seq(_)).getOrElse(
        sys.env.get("SCIPY_PIL_IMAGE_VIEWER").map(Seq(_)).getOrElse(
          defaultViewer))

    val pathWithExt = Path.createTempFile(
      suffix=this.preferredExt.getOrElse(null))
    path.copyTo(pathWithExt, replaceExisting=true)
    val proc: ProcessBuilder = (viewerBin ++ Seq(pathWithExt.path))
    println(s"Executing: ${proc}")
    proc.!
    if (needWait) {
      scala.io.StdIn.readLine("Press enter for deleting temporary files")
    }
  }
}

@WeaveFileType class PNGFile(p: Path) extends ImageFile(p) {
  override val contentType = "image/png"
  override val preferredExt = Some(".png")
}


@WeaveRecord class LinePlotData(
  val sources: WeaveSeq[p.TextFile],
  val legends: WeaveSeq[p.String],
  val styles: WeaveSeq[p.String]
) {
  def default =
    new LinePlotData(WeaveArray(), WeaveArray(), WeaveArray())

  def defaultStyle: String = {
    ""
  }

  def add(source: p.TextFile, legend: String, style: String) = 
    new LinePlotData(
      WeaveArray.fromSeq(this.sources.value ++ Seq(source)),
      WeaveArray.fromSeq(this.legends.value ++ Seq(new p.String(legend))),
      WeaveArray.fromSeq(this.styles.value ++ Seq(new p.String(style)))
    )
}

@WeaveRecord class LinePlotFigStyle(
  val xRange: WeaveOption[p.Tuple2[p.BigDecimal, p.BigDecimal]],
  val yRange: WeaveOption[p.Tuple2[p.BigDecimal, p.BigDecimal]],
  val size: WeaveOption[p.Tuple2[p.BigDecimal, p.BigDecimal]],
  val figTitle: WeaveOption[p.String],
  val xTitle: WeaveOption[p.String],
  val yTitle: WeaveOption[p.String],
  val xLog: p.Boolean,
  val yLog: p.Boolean) {

  def default = {
    new LinePlotFigStyle(
      new WeaveNoneT[p.Tuple2[p.BigDecimal, p.BigDecimal]](),
      new WeaveNoneT[p.Tuple2[p.BigDecimal, p.BigDecimal]](),
      new WeaveNoneT[p.Tuple2[p.BigDecimal, p.BigDecimal]](),
      new WeaveNoneT[p.String](),
      new WeaveNoneT[p.String](),
      new WeaveNoneT[p.String](),
      new p.Boolean(false),
      new p.Boolean(false))
  }
}

@WeaveFunction class AddYValuesToLinePlot {
  def apply(plot: LinePlotData, ys: WeaveSeq[p.BigDecimal],
    legend: p.String, style: p.String): LinePlotData = {
    val source = p.TextFile.output()
    source.path.write(ys.value.zipWithIndex.map({ case (v, idx) =>
      s"${idx}\t${v.toString}"
    }).mkString("\n"))
    plot.add(source, legend, style)
  }
}

@WeaveFunction class DrawLinePlotPNG {
  def apply(plot: LinePlotData, fig: LinePlotFigStyle)
      : PNGFile = {
    val output = PNGFile.output()
    val xRangeCmd =
      fig.xRange.value.map({r => s"set xrange [${r.value._1}:${r.value._2}]"})
        .getOrElse("")
    val yRangeCmd =
      fig.yRange.value.map({r => s"set yrange [${r.value._1}:${r.value._2}]"})
        .getOrElse("")
    val xTitleCmd = 
      fig.xTitle.value.map({t => s"set xlabel '${t.value}'"})
        .getOrElse("")
    val yTitleCmd =
      fig.yTitle.value.map({t => s"set ylabel '${t.value}'"})
        .getOrElse("")
    val figTitleCmd =
      fig.figTitle.value.map({t => s"set figlabel '${t.value}'"})
        .getOrElse("")

    val xLogCmd = if (fig.xLog.value) { "set logscale x" } else { "" }
    val yLogCmd = if (fig.yLog.value) { "set logscale y" } else { "" }

    val lineCmds =
      (for ((text, leg, sty) <-
        (plot.sources.value, plot.legends.value, plot.styles.value).zipped)
      yield {
        s"'${text.path.path}' using 1:2 with lines title '${leg}'"
      }).mkString(",")


    val script = s"""
set terminal png
set output '${output.path.path}'
${xRangeCmd}
${yRangeCmd}
${xTitleCmd}
${yTitleCmd}
${figTitleCmd}
${xLogCmd}
${yLogCmd}
plot ${lineCmds}
"""
    val scriptFile = p.TextFile.temporary()
    scriptFile.path.write(script)
    println("=== GNUPlot script ===\n" + script)
    val cmd = Seq("gnuplot", scriptFile.path.path)
    ProcessUtils.execute(cmd)
    output
  }

}
