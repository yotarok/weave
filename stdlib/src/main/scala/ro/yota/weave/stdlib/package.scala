package ro.yota.weave

import ro.yota.weave.{planned => p}
import scala.reflect.runtime.universe.TypeTag
import scalax.file.Path
import sys.process.ProcessBuilder

import scala.language.implicitConversions

package object stdlib {
  implicit def directoryOps(dir: Plan[p.Directory]): DirectoryOps = new DirectoryOps(dir)
  implicit def textFileOps[A <: p.TextFile : TypeTag](text: Plan[A]): TextFileOps[A] = new TextFileOps(text)


  implicit class ProcessLauncher(private val pb: ProcessBuilder)
      extends AnyVal {
    def exec(): Int = {
      ProcessUtils.execute(pb)
    }
  }

  implicit class ProcessBuildHelper(private val sc: StringContext)
      extends AnyVal {
    def cmd(args: Any*): ProcessBuilder = {
      import sys.process._
      val convertedArgs = args.map {
        case f: p.File => f.path.path
        case x: p.Primitive => x.value
        case c: p.Container => c.toString
        case p: Path => p.path
        case o => o
      }
      sc.s(convertedArgs:_*).stripMargin.replaceAll("\n", " ").trim
    }
  }

}
