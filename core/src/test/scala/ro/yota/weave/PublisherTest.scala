package ro.yota.weave.test

import ro.yota.weave._
import ro.yota.weave.storage._
import scalax.file.Path
import play.api.libs.json._
import play.api.libs.json.Json._
import org.scalatest.WordSpec

class PublisherSpec extends WordSpec {
  import io.findify.s3mock.S3Mock;
  val s3mock = S3Mock(port = 8002)
  s3mock.start

  val endpoint = new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8002", "us-west-2");

  val client = com.amazonaws.services.s3.AmazonS3ClientBuilder
    .standard()
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(endpoint)
    .withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(new com.amazonaws.auth.AnonymousAWSCredentials()))
    .build();
  client.createBucket("test-bucket-publisher")


  def locationsForFS(n: Int): Seq[String] = {
    for (i <- (0 until n)) yield {
      val tmppath = Path.createTempFile()
      tmppath.delete(true)
      val str = tmppath.path
      if (i % 2 == 0) {
        "file://" + str
      } else {
        str
      }
    }
  }


  def locationsForS3(n: Int): Seq[String] = {
    for (i <- (0 until n)) yield {
      s"s3://test-bucket-publisher/test/location/${i}"
    }
  }

  val rng = scala.util.Random

  def makeRandomFile(): (Path, String) = {
    val s = rng.nextInt.toString
    val tmppath = Path.createTempFile()
    tmppath.write(s)
    (tmppath, s)
  }

  val impls: Seq[(String, PublisherImpl, (Int => Seq[String]))] = Seq(
    ("S3Publisher", new S3Publisher(Json.obj("s3EndPoint" -> "http://127.0.0.1:8002")), locationsForS3),
    ("FSPublisher", new FSPublisher(Json.obj()), locationsForFS)
  )

  for ((typeName, impl, locsgen) <- impls) {
    typeName should {
      "be able to store and load a file" in {
        val locs = locsgen(10)
        for (loc <- locs) {
          val (tmppath, s) = makeRandomFile()

          assert(! impl.exists(loc))
          assert(impl.retrieve(loc).isEmpty)

          impl.publish(loc, tmppath)
          assert(impl.exists(loc))

          val retrieved = impl.retrieve(loc)

          assert(! retrieved.isEmpty)

          assert (s == retrieved.get.string)
        }

        // test overwrite
        for (loc <- locs) {
          val (tmppath, s) = makeRandomFile()
          tmppath.write(s)
          assert(impl.exists(loc))
          impl.publish(loc, tmppath)

          val retrieved = impl.retrieve(loc)
          assert(! retrieved.isEmpty)
          assert (s == retrieved.get.string)
        }


      }
    }
  }
}
