package ro.yota.weave

import ro.yota.weave.{planned => p}
import scalax.file.Path
import scalax.io.Resource
import scala.util.matching.Regex
import scala.util.control.Breaks
import play.api.libs.json.{Json,JsObject,JsArray}

import com.amazonaws.services.s3.AmazonS3Client
import io.atlassian.aws.{AmazonClient, AmazonClientConnectionDef, Credential}
import io.atlassian.aws.s3.{S3, ContentLocation, Bucket, S3Key, S3Action}


/** Simple file storage */
trait PublisherImpl {
  def publish(loc: String, file: Path)
  def exists(loc: String): Boolean
  def retrieve(loc: String): Option[Path]
  def locationPattern: Regex
}

/** Manager for multiple publishers
  * This class also act as a publisher with delegating the actual processes
  */
class Publisher(private val impls: Seq[PublisherImpl]) {
  /** Find first implementation matches to location pattern
    */
  private[this] def findFirstImpl(loc: String): Option[PublisherImpl] =
    impls.filterNot({ impl =>
      impl.locationPattern.findAllMatchIn(loc).isEmpty
    }).headOption

  /** Do something with the first impl, and throws exception if there's no
    *  impl for location
    */
  private[this] def performWithFirstImpl[A](loc: String)(f: PublisherImpl => A) =
    findFirstImpl(loc) match {
      case Some(impl) => f(impl)
      case None => throw new RuntimeException(s"Publisher not found for ${loc}")
    }

  def publish(loc: String, file: Path) {
    performWithFirstImpl(loc)(impl => impl.publish(loc, file))
  }

  def exists(loc: String): Boolean = {
    performWithFirstImpl(loc)(impl => impl.exists(loc))
  }

  def retrieve(loc: String): Option[Path] = {
    findFirstImpl(loc) match {
      case Some(impl) => impl.retrieve(loc)
      case None => None
    }
  }
}

object Publisher {
  def createPublisher(configs: JsArray) = {
    val impls = for (v <- configs.value) yield {
      val config = v.as[JsObject]
      val klassName = (config \ "className").as[String]
      val ctr = Class.forName(klassName).getConstructor(
        classOf[JsObject])
      ctr.newInstance(config).asInstanceOf[PublisherImpl]
    }
    new Publisher(impls)
  }
  def createDefaultPublisher() = {
    new Publisher(Seq(new FSPublisher()))
  }
}

/** Publisher to a file system
  *
  * This requires shared file system if it is used in distributed environment
  */
class FSPublisher(config: JsObject) extends PublisherImpl {
  def locationPattern = ".*".r

  def this() {
    this(Json.obj())
  }

  private[this] def removeScheme(s: String) =
    if (s.startsWith("file://")) {
      s.substring(7)
    } else { s }

  def publish(dest: String, file: Path) {
    val destpath = Path.fromString(removeScheme(dest))
    file.copyTo(destpath, replaceExisting = true)
  }

  def exists(loc: String): Boolean = {
    Path.fromString(removeScheme(loc)).exists
  }

  def retrieve(loc: String): Option[Path] = {
    val p = Path.fromString(removeScheme(loc))
    if (p.exists) {
      Some(p)
    } else {
      None
    }
  }

}

/** Publisher to S3
  *
  */
class S3Publisher(config: JsObject) extends PublisherImpl {
  val locationPattern = "s3://([^/]+)/(.*)$".r

  val awsConfig = AmazonClientConnectionDef.default.copy(
    endpointUrl=Some((config \ "s3EndPoint").as[String]),
    credential=None
  )
  val awsClient = AmazonClient.withClientConfiguration[AmazonS3Client](awsConfig)

  /** Return ContentLocation for the location string
    */
  def location(s: String) = {
    s match {
      case locationPattern(bucket, key) => {
        ContentLocation(Bucket(bucket), S3Key(key))
      }
      case _ => {
        throw new RuntimeException(s"Invalid location ${s}")
      }
    }
  }

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

  def runS3ActionOrError[A](a: S3Action[A]): Either[Throwable, A] = {
    val (meta, errorOrNot) = a.run(awsClient).run.run.run
    errorOrNot match {
      case scalaz.-\/(err) => {
        Left(err.toThrowable.getCause)
      }
      case scalaz.\/-(res) => {
        Right(res)
      }
    }
  }

  def publish(dest: String, file: Path) {
    val loc = location(dest)
    runS3Action { S3.putFile(loc, file.fileOption.get) }
  }

  def exists(dest: String): Boolean = {
    val loc = location(dest)
    runS3Action { S3.exists(loc) }
  }

  def retrieve(dest: String): Option[Path] = {
    val loc = location(dest)
    (runS3ActionOrError { S3.get(loc) }) match {
      case Right(obj) => {
        val input = Resource.fromInputStream(obj.getObjectContent())
        val tmppath = Path.createTempFile()
        input.copyDataTo(tmppath)
        Some(tmppath)
      }
      case Left(exc) => {
        None
      }
    }
  }

}
