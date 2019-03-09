package ro.yota.weave.storage

import scalax.file.Path
import scalax.file.ImplicitConversions._
import play.api.libs.json._
import play.api.libs.json.Json._
import java.io.File
import scalax.io.{Output, Input, Codec, Resource}
import sys.process._
import java.io.{InputStream,OutputStream}
import java.security.MessageDigest
import scala.util.matching.Regex
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging.Logger

import ro.yota.weave.{planned => p}
import ro.yota.weave.{Plannable, Hasher}
import com.typesafe.scalalogging.Logger

class LogType
case object LogType_Error extends LogType
case object LogType_Output extends LogType


/**
  * Base class for key set
  */
trait StorageKeySet {
  def exists(key: String) : Boolean
}

case class DefaultStorageKeySet(storage: Storage)
    extends StorageKeySet {

  def exists(key: String) : Boolean = {
    storage.exists(key)
  }
}

/**
  * Base traits for all storage classes
  */
trait Storage {

  /** snapshot of keyset efficient for querying multiple objects*/
  def keySet: StorageKeySet = DefaultStorageKeySet(this)

  def id : String
  def config : JsObject

  /**
    * Check whether the object specified by the key exists
    */
  def exists(key: String) : Boolean

  /**
    * Complete key string from prefix
    *
    * @param prefix prefix of key
    * @param searchLog if true, search stdout log directory instead of info directoruy
    * @return keys starting with the given prefix
    */
  def complete(prefix: String, searchLog: Boolean = false): Set[String]

  /**
    * Store jar file and returns key for referencing it later
    */
  def storeJar(jarPath: Path, overwrite: Boolean = false): String
  /**
    * Download jar file from the key, and return path for a temporary file
    */
  def loadJar(digest: String): Path

  def storeInfo(key: String, info: JsObject)
  def storeObjectBody(key: String, path: Path)

  def loadInfo(key: String): JsObject
  def loadObjectBody(key: String, destpath: Path)

  // return None for nonprimitives, return temppath for remote storage
  def getObjectPath(key: String): Option[Path]

  def loadLog(key: String, logType: LogType): Option[Path]
  def storeLog(key: String, logType: LogType, logPath: Path)

  def searchCache(key: String): Option[String]
  def addCache(key: String, objectKey: String)

  def deleteObject(key: String)

  /** remove all objects and logs, mainly for test */
  def destroy()

  def logger: Logger = Logger("ro.yota.weave.storage.Storage")
}


object Storage {
  val defaultLogger = Logger("ro.yota.weave.storage.Storage")
  def createStorage(storageConfig : JsObject)
    (implicit logger: Logger = defaultLogger) : Storage = {

    val className = (storageConfig \ "className").as[String]
    val ctr = Class.forName(className).getConstructor(
      classOf[JsObject], classOf[Logger])
    ctr.newInstance(storageConfig, logger).asInstanceOf[Storage]
  }
}


/**
  * The base class for back-end key-value store
  *
  * Weave Storage is also a key-value store bust with different value types.
  * This class provides an access to a backend simple (type-agnostic) key-value
  *  store.
  *
  * Type parameter K is a type for keys. For example, this will be Path
  *  and ContentLocation in FSStorage and S3Storage, respectively.
  */
trait BackendKVS[K] {
  def exists(key: K): Boolean
  def getFile(key: K): Path
  def getStream[A](key: K)(f: (InputStream=>A)): A
  def putFile(key: K, path: Path): Unit
  def putStream[A](key: K)(f: (OutputStream=>A)): A

  def delete(key: K): Unit

  def getFileOpt(key: K): Option[Path] =
    if (exists(key)) {
      Some(getFile(key))
    } else {
      None
    }

  def getStreamOpt[A](key: K)(f: (InputStream=>A)): Option[A] =
    if (exists(key)) {
      Some(getStream(key)(f))
    } else {
      None
    }
}

/**
  * Implementation for a storage that is implemented based on other KVS system
  *
  * This implements default behavior of storage classes, therefore,
  * by inheriting this, a concrete class doesn't need to implement logic.
  * Concrete class is responsible to
  *  - Converting object key to backend-specific key type (e.g. S3 URI or Path)
  *  - Prepare write/ read streams for a specified key
  */
trait KeyedStorage[K] extends Storage {
  type Key = K
  def backend: BackendKVS[K]

  def readerPipeCommand: Option[String]
  def writerPipeCommand: Option[String]

  def readerPipe[A](f: (InputStream=>A)): (InputStream=>A) =
    readerPipeCommand match {
      case Some(cmd) => {
        { (istream: InputStream) =>
          val shcmd = Seq("sh", "-c", cmd)
          val pin = new java.io.PipedInputStream()
          val pout = new java.io.PipedOutputStream(pin);
          val procf: Future[Int] = Future {
            (shcmd #< istream #> pout).!;
          }
          procf onComplete { r =>
            pout.close()
          }
          val ret = f(pin)
          pin.close()
          ret
        }
      }
      case None => f
    }

  def writerPipe[A](f: (OutputStream=>A)): (OutputStream=>A) =
    writerPipeCommand match {
      case Some(cmd) => {
        { (ostream: OutputStream) =>
          val shcmd = Seq("sh", "-c", cmd)
          val pout = new java.io.PipedOutputStream()
          val pin = new java.io.PipedInputStream(pout)
          val proc = (shcmd #< pin #> ostream).run();
          val ret = f(pout);
          pout.close();
          val procExit = proc.exitValue();
          ret
        }
      }
      case None => f
    }

  def keyForLog(logType: LogType, keyStr: String): Key
  def keyForJar(keyStr: String): Key
  def keyForInfo(keyStr: String): Key
  def keyForObject(keyStr: String): Key
  def keyForCache(keyStr: String): Key

  def addToKeyCache(keyStr: String): Unit = {
    // Only applicable in cached storage
  }

  def exists(key: String): Boolean = {
    backend.exists(keyForInfo(key))
  }

  def storeJar(jarPath: Path, overwrite: Boolean): String = {
    val hash = Hasher(jarPath)
    val jarKey = keyForJar(hash)
    if (! backend.exists(jarKey)) {
      backend.putFile(jarKey, jarPath)
    }
    hash
  }
  def loadJar(digest: String): Path = {
    backend.getFile(keyForJar(digest))
  }

  def storeInfo(key: String, info: JsObject) {
    logger.info(s"Trying to store info for ${key}...")
    backend.putStream(keyForInfo(key)) { writerPipe { ostream =>
      ostream.write(Json.stringify(info).getBytes)
    }}
    logger.info(s"done...")
  }

  def storeObjectBody(keyStr: String, path: Path) {
    val key: K = keyForObject(keyStr)
    backend.putStream(key) { writerPipe { ostream =>
      path copyDataTo Resource.fromOutputStream(ostream)
    }}
  }



  def loadInfo(keyStr: String): JsObject = {
    val jstr = backend.getStream(keyForInfo(keyStr)) { readerPipe { istream =>
      Resource.fromInputStream(istream).string
    }}
    Json.parse(jstr).asInstanceOf[JsObject]
  }

  def loadObjectBody(keyStr: String, destpath: Path) {
    val key: K = keyForObject(keyStr)
    backend.getStream(key) { readerPipe { istream =>
      Resource.fromInputStream(istream) copyDataTo destpath.outputStream()
    }}
  }

  /**
    * Return None for nonprimitives, return temppath for remote storage
    *
    * This function is used especially in web interface
    */
  def getObjectPath(keyStr: String): Option[Path] = {
    val info = this.loadInfo(keyStr)
    if ((info \ "isPrimitive").as[Boolean]) {
      val objectKeyStr = (info \ "objectRef").asOpt[String].getOrElse(keyStr)
      val objectKey = keyForObject(objectKeyStr)
      val tmppath =
        backend.getStream(objectKey) { readerPipe { istream =>
          val t = Path.createTempFile()
          Resource.fromInputStream(istream) copyDataTo t.outputStream()
          t
        }}
      //readerPipe(objectKey) copyDataTo tmppath.outputStream()
      Some(tmppath)
    } else {
      None
    }
  }

  def loadLog(key: String, logType: LogType): Option[Path] = {
    backend.getStream(keyForLog(logType, key)) { readerPipe { istream =>
      val ret = Path.createTempFile()
      Resource.fromInputStream(istream) copyDataTo (ret.outputStream())
      Some(ret)
    }}
    /*
    for {
      stream <- backend.getStreamOpt(keyForLog(logType, key));
      pipe = readerPipe(stream)
    } yield {
      val ret = Path.createTempFile()
      pipe copyDataTo (ret.outputStream())
      ret
    }
     */
  }

  def storeLog(key: String, logType: LogType, path: Path) {
    val k = keyForLog(logType, key)
    backend.putStream(k) { writerPipe { (ostream: OutputStream) =>
      path copyDataTo Resource.fromOutputStream(ostream)
    }}
  }

  def searchCache(keyStr: String): Option[String] = {
    val cacheKey = keyForCache(keyStr)
    if (backend.exists(cacheKey)) {
      val objRef = backend.getStream(cacheKey) { istream =>
        Resource.fromInputStream(istream).string.trim
      }
      // val istream = backend.getStream(keyForCache(keyStr))
      //Resource.fromInputStream(istream).string.trim
      Some(objRef)
    } else {
      None
    }
  }

  def addCache(keyStr: String, objectKeyStr: String): Unit = {
    val cacheKey = keyForCache(keyStr)
    backend.putStream(cacheKey) { ostream =>
      ostream.write(objectKeyStr.getBytes)
    }
  }

  def deleteObject(key: String) {
    backend.delete(keyForObject(key))
    backend.delete(keyForInfo(key))
  }

}
