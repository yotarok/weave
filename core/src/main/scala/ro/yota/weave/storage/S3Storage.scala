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
import scala.concurrent.{Future,Await}
import scala.concurrent.duration.Duration

import com.amazonaws.services.s3.AmazonS3Client
import io.atlassian.aws.{AmazonClient, AmazonClientConnectionDef, Credential}
import io.atlassian.aws.s3.{S3, ContentLocation, Bucket, S3Key, S3Action}
import com.amazonaws.services.s3.model.{ObjectListing, ObjectMetadata}

import ro.yota.weave.{planned => p}
import ro.yota.weave.{Hasher,Plannable}
import com.typesafe.scalalogging.Logger

case class S3StorageKeySnapshot(val storage: S3Storage)  extends StorageKeySet {
  // This version of snapshot initiates 16 parallel list requests
  // and obtain all the keys.
  // TO DO: Compare performance and cost with the previous version which
  //  performs GET request for each key
  import scala.concurrent.ExecutionContext.Implicits.global

  val subsets: Seq[Future[Set[String]]] = (0 until 16).map(_.toHexString).map { (ch: String) =>
    S3.listKeys(storage.bucket, storage.rootKeyStr + "info/" + ch)
  }.map { (act: S3Action[ObjectListing]) =>
    Future {
      storage.extractKeysFromListing(storage.runS3Action(act))
    }
  }

  def exists(key: String) : Boolean = {
    val setIdx = Integer.parseInt(key.substring(0, 1), 16)
    val set = Await.result(subsets(setIdx), Duration.Inf)
    set.contains(key)
  }
}

/**
  *  Storage implementation with backend key-value-store on S3
  */
class S3Storage(val config: JsObject)
  (implicit override val logger: Logger = Logger(classOf[S3Storage]))
    extends KeyedStorage[ContentLocation] {

  val rootPath : String = (config \ "rootPath").as[String]
  assert (rootPath.startsWith("s3://"))
  val id = Hasher(rootPath.path)

  val readerPipeCommand = (config \ "readerPipe").asOpt[String]
  val writerPipeCommand = (config \ "writerPipe").asOpt[String]

  def runS3Action[A](a: S3Action[A]): A = {
    val (meta, errorOrNot) = a.run(awsClient).run.run.run
    errorOrNot match {
      case scalaz.-\/(err) => {
        throw new RuntimeException(err.toString)
      }
      case scalaz.\/-(res) => {
        res
      }
    }
  }

  val backend = new BackendKVS[ContentLocation] {

    def exists(loc: ContentLocation): Boolean = runS3Action { S3.exists(loc) }
    def getFile(loc: ContentLocation): Path = {
      val tmppath = Path.createTempFile()
      getStream(loc) { istream =>
        val inp = Resource.fromInputStream(istream)
        inp copyDataTo (tmppath.outputStream())
      }
      tmppath
    }

    def getStream[A](loc: ContentLocation)(f: (InputStream=>A)): A = {
      val obj = runS3Action { S3.get(loc) };
      f(obj.getObjectContent())
    }

    def putFile(loc: ContentLocation, path: Path) {
      val meta = new ObjectMetadata()
      path.size match {
        case Some(s) => {
          println(s"Content size is set to ${s}")
          meta.setContentLength(s)
        }
        case _ => {}
      }
      val a = runS3Action { S3.putFile(loc, path.fileOption.get, meta) }
      println(s"${a}")
    }

    def putStream[A](loc: ContentLocation)(f: (OutputStream=>A)) = {
      import scalax.io.StandardOpenOption._
      val tmppath = Path.createTempFile()
      val ret = tmppath.outputStream(Create, Write).acquireFor(f) match {
        case Left(x) => { throw x.head }
        case Right(x) => x
      }
      putFile(loc, tmppath)
      ret
    }

    def delete(loc: ContentLocation) {
      runS3Action {
        S3.delete(loc)
      }
    }
  }

  val credential = if (config.keys.contains("_accessKey")) {
    Some(Credential.static(
      (config \ "_accessKey").as[String],
      (config \ "_secretKey").as[String]))
  } else {
    None
  }

  val awsConfig = AmazonClientConnectionDef.default.copy(
    endpointUrl=(config \ "s3EndPoint").asOpt[String],
    credential=credential
  )
  val awsClient = AmazonClient.withClientConfiguration[AmazonS3Client](awsConfig)

  override def keySet: StorageKeySet =
    S3StorageKeySnapshot(this)

  val rexpS3Uri = """s3://([^/]+)/(.*)$""".r
  val (bucket, rootKeyStr) = rootPath match {
    case rexpS3Uri(bucket, key) => {
      (Bucket(bucket), if (key.endsWith("/")) { key } else { key + "/" })
    }
  }

  def locationFor(typeStr: String, key: String, suffix: String = ""): ContentLocation = {
    ContentLocation(bucket, S3Key(rootKeyStr + typeStr + "/" + key + suffix))
  }

  def keyForLog(logType: LogType, keyStr: String): ContentLocation = {
    val typeName = logType match {
      case LogType_Error => "err"
      case LogType_Output => "out"
    }
    locationFor("log/" + typeName, keyStr)
  }
  def keyForJar(keyStr: String): ContentLocation = {
    locationFor("jar", keyStr)
  }
  def keyForInfo(keyStr: String): ContentLocation = {
    locationFor("info", keyStr)
  }
  def keyForObject(keyStr: String): ContentLocation = {
    locationFor("object", keyStr)
  }
  def keyForCache(keyStr: String): ContentLocation = {
    locationFor("cache", keyStr)
  }

  def extractKeysFromListing(olist: ObjectListing): Set[String] = {
    import scala.collection.JavaConverters._
    var keySet: Set[String] = Set()
    var continue = true
    while (continue) {
      val s:Set[String] = olist.getObjectSummaries().asScala.map({ m =>
        m.getKey().substring(m.getKey().lastIndexOf('/') + 1)
      }).toSet[String]
      keySet = keySet ++ s
      continue = olist.isTruncated()
    }
    keySet
  }

  def complete(prefix: String, searchLog: Boolean = false): Set[String] = {
    val dirPrefix = if (searchLog) {
      rootKeyStr + "log/out"
    } else {
      rootKeyStr + "info"
    }
    val keyPrefix = dirPrefix + "/" + prefix
    val olist = runS3Action {
      S3.listKeys(bucket, keyPrefix)
    }
    extractKeysFromListing(olist)
  }

  def destroy() {
    import scala.collection.JavaConverters._

    var olist = runS3Action { S3.listKeys(bucket, rootKeyStr) }
    olist.getObjectSummaries().asScala.foreach { summary =>
      runS3Action {
        S3.delete(ContentLocation(bucket, S3Key(summary.getKey())))
      }
    }

    while (olist.isTruncated()) {
      olist = runS3Action { S3.nextBatchOfKeys(olist) }
      olist.getObjectSummaries().asScala.foreach { summary =>
        runS3Action {
          S3.delete(ContentLocation(bucket, S3Key(summary.getKey())))
        }
      }
    }

  }

  /*
  def enumerateAllKeys: Iterator[String] = {
    import scala.collection.JavaConverters._

    val ret = scala.collection.mutable.ListBuffer.empty[String]
    var olist = runS3Action { S3.listKeys(bucket, "info/") }
    ret ++= olist.getObjectSummaries().asScala.map { _.getKey().replace("info/", "") }
    while (olist.isTruncated()) {
      olist = runS3Action { S3.nextBatchOfKeys(olist) }
      ret ++= olist.getObjectSummaries().asScala.map { _.getKey().replace("info/", "") }
    }
    ret.toIterator
  }
   */

}

