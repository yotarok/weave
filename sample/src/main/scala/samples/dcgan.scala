package exp

import ro.yota.weave._
import ro.yota.weave.macrodef._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import scalax.file.{Path,PathSet}
import scala.language.implicitConversions

@WeaveRecord class DataPreparationOpts(
  val width: p.Int,
  val height: p.Int,
  val originalExt: p.String,
  val targetExt: p.String
)

@WeaveFunction class PrepareImage {
  override def version = "2018-05-07"
  def apply(opts: DataPreparationOpts, input: p.Directory): p.Directory = {
    val output = p.Directory.output()
    val allImgs: PathSet[Path] = input.path.descendants((p: Path) =>
      p.name.endsWith(opts.originalExt.value) && ! p.name.startsWith("."))

    val digits = scala.math.ceil(scala.math.log10(allImgs.size)).toInt
    val zeros = "0" * digits

    val filenames = for ((path, idx) <- allImgs.toSeq.zipWithIndex) yield {
      val dest = (zeros + idx.toString).takeRight(digits) + opts.targetExt
      cmd"""
convert ${path.path} -resize ${opts.width.value}^x${opts.height.value}^ ${output.path.path}/${dest}
""".exec()
      dest
    }

    (output.path / "filelist.txt").writeStrings(filenames, "\n")

    output
  }
}

object DCGAN extends Project {
  // ImageDirectory
  val dir = configVar("DCGAN_DATA_ROOT").as[String]
  val originalDir = injectDirectory(Path.fromString(dir))

  val prepOpts = DataPreparationOpts(
    128, 128, ".jpg", ".png"
  )
  val preparedDir = PrepareImage(prepOpts, originalDir)
    .checkPoint("oahgan/prepared_dataset.tar")
  // Square

  val prepOptsSR = DataPreparationOpts(
    512, 512, ".jpg", ".png"
  )

  val preparedDirSR = PrepareImage(prepOptsSR, originalDir)
    .checkPoint("oahgan/prepared_dataset.SR.tar")

}
