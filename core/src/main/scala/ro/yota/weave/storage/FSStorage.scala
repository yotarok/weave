package ro.yota.weave.storage

import scalax.file.{Path, PathMatcher}
import scalax.file.ImplicitConversions._
import play.api.libs.json._
import play.api.libs.json.Json._
import java.io.File
import scalax.io.{Output, Input, Codec, Resource}
import sys.process._
import java.io.{InputStream,OutputStream}
import java.security.MessageDigest
import scala.util.matching.Regex

import ro.yota.weave.{planned => p}
import ro.yota.weave.{Hasher,Plannable}
import com.typesafe.scalalogging.Logger

/**
  * storage implementation with backend key-value-store on file system
  */
class FSStorage(val config: JsObject)
  (implicit override val logger: Logger = Logger(classOf[FSStorage]))
    extends KeyedStorage[Path] {

  val rootPath : Path = Path.fromString((config \ "rootPath").as[String])
  val id = Hasher(rootPath.path)

  val readerPipeCommand = (config \ "readerPipe").asOpt[String]
  val writerPipeCommand = (config \ "writerPipe").asOpt[String]

  val backend = new BackendKVS[Path] {
    /// Ensure that the specified directory exists
    def ensureDir(dir: Path): Path = {
      if (! dir.exists) { new File(dir.path).mkdirs() }
      dir
    }

    /// Ensure that the parent directory of the specified path exists
    def ensureParentDir(path: Path): Option[Path] =
      for {p <- path.parent} yield {ensureDir(p)}

    def exists(key: Path) = key.exists
    def getFile(key: Path) = key

    def getStream[A](path: Path)(f: (InputStream=>A)): A = {
      path.inputStream.acquireFor(f) match {
        case Left(x) => { throw x.head }
        case Right(x) => x
      }
    }
    def putFile(key: Path, path: Path) {
      ensureParentDir(key)
      path.copyTo(key, replaceExisting=true)
    }
    def putStream[A](key: Path)(f: (OutputStream=>A)): A = {
      import scalax.io.StandardOpenOption._
      key.outputStream(CreateFull, Write).acquireFor(f) match {
        case Left(x) => { throw x.head }
        case Right(x) => x
      }
    }

    def delete(key: Path) {
      key.delete(false)
    }
  }

  /** Resolve path from given key, subdir, and suffix */
  def keyToPathPrefix(key: String, subdir: String, suffix: String="") : Path = {
    val dir = subdir.split("/").foldLeft(rootPath) { (p, d) =>
      p / d
    }
    dir / key.substring(0, 2) / (key + suffix)
  }

  def keyForLog(logType: LogType, keyStr: String): Path = {
    val typeName = logType match {
      case LogType_Error => "err"
      case LogType_Output => "out"
    }
    keyToPathPrefix(keyStr, "log/" + typeName)
  }
  def keyForJar(keyStr: String): Path = {
    keyToPathPrefix(keyStr, "jar", ".jar")
  }
  def keyForInfo(keyStr: String): Path = {
    keyToPathPrefix(keyStr, "info")
  }
  def keyForObject(keyStr: String): Path = {
    keyToPathPrefix(keyStr, "object")
  }
  def keyForCache(keyStr: String): Path = {
    keyToPathPrefix(keyStr, "cache")
  }

  def complete(prefix: String, searchLog: Boolean = false): Set[String] = {
    val searchPath = if (searchLog) {
      rootPath / "log" / "out"
    } else {
      rootPath / "info"
    }
    val names: Seq[String] =
      searchPath.descendants(
        PathMatcher.GlobNameMatcher(prefix + "*"), 2)
        .toSeq
        .filter({path:Path => path.isFile})
        .map({path:Path => path.name})
    names.toSet[String]
  }

  def destroy() {
    rootPath.deleteRecursively(false)
  }

}

