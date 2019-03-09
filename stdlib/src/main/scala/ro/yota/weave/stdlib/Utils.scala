package ro.yota.weave.stdlib

import sys.process._
import ro.yota.weave.{planned => p}
import scalax.file.Path
import java.nio.file

object ProcessUtils {
  /**
    * Invoke the process and redirects the output to the default pager
    */
  def pipeToPager(cmd: ProcessBuilder): Int = {
    println(s"Executing: ${cmd} (with pager)")
    val pagerBin = sys.env.getOrElse("PAGER", "more")
    val retcode = (cmd #| Seq("sh", "-c", pagerBin + " > /dev/tty")).!;
    retcode
  }


  /**
    * Invoke the process with logging
    */
  def execute(cmd: ProcessBuilder, allowFail: Boolean=false): Int = {
    println(s"Executing: ${cmd}")
    val retcode = cmd.!;
    println(s"Return code = ${retcode}")
    if (! allowFail && retcode != 0) {
      throw new RuntimeException(s"Command ${cmd} failed with retcode=${retcode}")
    }
    retcode
  }

}


object ResourceUtils {
  /**
    * Extract resources and returns a directory path
    */
  def extract(klass: Class[_], dirName: String): Path = {
    import java.nio.file.{Path, Paths, FileSystems, Files, SimpleFileVisitor}
    import java.nio.file.{FileVisitResult, StandardCopyOption}
    import java.nio.file.attribute.{BasicFileAttributes}
    import java.util.Collections

    val trimmedDirName = if (dirName.endsWith("/")) {
      dirName.slice(0, dirName.length - 1)
    } else {
      dirName
    }

    val uri = klass.getResource(trimmedDirName + "/").toURI
    val srcroot: Path = if (uri.getScheme == "jar") {
      val fileSystem =
        FileSystems.newFileSystem(
          uri, Collections.emptyMap[String, Object]())
      fileSystem.getPath(trimmedDirName)
    } else {
      Paths.get(uri)
    }

    val tempdir = p.Directory.temporary()
    val dstroot = Paths.get(tempdir.path.path)

    println(s"Start copying tree: ${srcroot} => ${dstroot}")

    val visitor = new SimpleFileVisitor[Path]() {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes)
          : FileVisitResult = {
        // copy a directory
        if (! dir.equals(srcroot)) {
          val dest = dstroot.resolve(srcroot.relativize(dir).toString);

          println(s"Copy directory: ${dir} => ${dest}")
          Files.copy(dir, dest, StandardCopyOption.COPY_ATTRIBUTES);
        }
        FileVisitResult.CONTINUE
      }
      override def visitFile(file: Path, attrs: BasicFileAttributes)
          : FileVisitResult = {
        // copy a file
        val dest = dstroot.resolve(srcroot.relativize(file).toString);
        println(s"Copy file: ${file} => ${dest}")
        Files.copy(file, dest, StandardCopyOption.COPY_ATTRIBUTES);
        FileVisitResult.CONTINUE
      }
    };

    Files.walkFileTree(srcroot, visitor);

    tempdir.path
  }
}
