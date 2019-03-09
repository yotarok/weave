package ro.yota.weave.test

import ro.yota.weave._
import ro.yota.weave.storage._
import scalax.file.Path
import play.api.libs.json._
import play.api.libs.json.Json._
import org.scalatest.WordSpec

/** Builder for storages intended to be used by tests */
trait StorageBuilder[B] {
  def updateBuilder(o: JsObject): B
  def build(): Storage
  def config: JsObject

  def withReaderPipe(cmd: String) =
    updateBuilder(config ++ Json.obj("readerPipe" -> cmd))

  def withWriterPipe(cmd: String) =
    updateBuilder(config ++ Json.obj("writerPipe" -> cmd))

}

case class FSStorageBuilder(val config: JsObject = Configs.baseFSStorage)
    extends StorageBuilder[FSStorageBuilder] {
  def updateBuilder(o: JsObject) = FSStorageBuilder(o)
  def build() = new FSStorage(config)

  def withTempDirectory = {
    val rootPath = scalax.file.FileSystem.default.createTempDirectory()
    updateBuilder(config ++ Json.obj("rootPath" -> rootPath.path))
  }
}

case class S3StorageBuilder(val config: JsObject = Configs.baseS3Storage)
    extends StorageBuilder[S3StorageBuilder] {
  def updateBuilder(o: JsObject) = S3StorageBuilder(o)
  def build() = new S3Storage(config)

  def withRootPath(s: String) = {
    updateBuilder(config ++ Json.obj("rootPath" -> s))
  }
  def withEndPoint(s: String) = {
    updateBuilder(config ++ Json.obj("s3EndPoint" -> s))
  }
}


object Configs {
  val baseFSStorage = Json.obj("className" -> "ro.yota.weave.storage.FSStorage")

  val baseS3Storage = Json.obj("className" -> "ro.yota.weave.storage.S3Storage")
}

class StorageSpec extends WordSpec {
  import io.findify.s3mock.S3Mock;
  val s3mock = S3Mock(port = 8001)
  s3mock.start
  val endpoint = new com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8001", "us-west-2");
  val client = com.amazonaws.services.s3.AmazonS3ClientBuilder
    .standard()
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(endpoint)
    //.withCredentials(new com.amazonaws.auth.AWSStaticCredentialsProvider(new com.amazonaws.auth.AnonymousAWSCredentials()))
    .build();
  client.createBucket("test-bucket")

  val storages = Seq(
    ("Plain FileSystem Storage",
      FSStorageBuilder().withTempDirectory.build()),
    ("Piped FileSystem Storage",
      FSStorageBuilder()
        .withTempDirectory
        .withReaderPipe("zcat")
        .withWriterPipe("gzip -c")
        .build()
    ),
    ("Plain S3 Storage",
      S3StorageBuilder()
        .withEndPoint("http://127.0.0.1:8001")
        .withRootPath("s3://test-bucket/testprefix/")
        //.withAnonymousCredential()
        .build()),
    ("Piped S3 Storage",
      S3StorageBuilder()
        .withEndPoint("http://127.0.0.1:8001")
        .withRootPath("s3://test-bucket/testprefix_with_pipe/")
        //.withAnonymousCredential()
        .withReaderPipe("zcat")
        .withWriterPipe("gzip -c")
        .build())
  )

  for ((typeName, storage) <- storages) {
    typeName should {
      "be able to store info and respond to existence check" in {
        storage.destroy()
        val k1 = "8df7f638da50ddfa8f6a4162ddfc738b65e8b1cf"
        assert(! storage.exists(k1))
        storage.storeInfo(k1, Json.obj("foo" -> 1))
        assert(storage.exists(k1))
        assert((storage.loadInfo(k1) \ "foo").asOpt[Int] == Some(1))
      }

      "be able to store object" in {
        storage.destroy()
        val k1 = "8df7f638da50ddfa8f6a4162ddfc738b65e8b1aa"
        assert(! storage.exists(k1))

        val tmppath = Path.createTempFile()
        val content = "Dummy object"
        tmppath.write(content)

        storage.storeObjectBody(k1, tmppath)

        // storing object doesn't change object existence flag
        assert(! storage.exists(k1))

        val tmppath2 = Path.createTempFile()
        storage.loadObjectBody(k1, tmppath2)

        assert(tmppath2.string == content)
      }

      "be able to be searched via keysets" in {
        storage.destroy()
        val dummyInfo = Json.obj("foo" -> 1)
        val testKeys: Seq[String] = Seq(
          "8df7f638da50ddfa8f6a4162ddfc738b65e8b1cf",
          "006b466749dafbda4ea155057bda63c52d83542a",
          "4a3730defed9b59da5c7dba0eee1f8d02cab0a54",
          "7291a3368fd050b37c47e2dcae079912272374a2",
          "f33f55f99283b758f71985a61a8d6efa0559dd31",
          "2dda89c4f01b03f1c0a0a3e637e71e2ded25c1bc",
          "d6f48eed00d4d30ae0a8af06d02fa23db22d8413",
          "c9d990f28a0fccd20c4a5094067bac92b2f42d69",
          "8ebc4544b9ae43bb603cf456c32d1d8b5e337f9c",
          "0d973df0734052a0dc1a36536966f8c67d713326"
        )
        val qKey = "866631394e8e61e26c59ddbe51a184231db4c175"
        for (testKey <- testKeys) {
          storage.storeInfo(testKey, dummyInfo)
        }
        val keyset = storage.keySet
        for (testKey <- testKeys) {
          assert(keyset.exists(testKey))
        }
        assert(! keyset.exists(qKey));
      }

      "be able to store jars" in {
        val tmppath = Path.createTempFile()
        val content = "TEST"
        tmppath.write(content)

        val digest = storage.storeJar(tmppath)

        assert(digest == Hasher(content))

        assert(storage.loadJar(digest).string == content)
      }

      "be able to complete keys from prefixes" in {
        storage.destroy()
        val dummyInfo = Json.obj("foo" -> 1)
        val dummyLog = Path.createTempFile()
        dummyLog.write("DUMMY")
        val testKeys: Seq[String] = Seq(
          "7ac8c4dbd3c61885c40cc3a9c2d02f99b3eaf94b",
          "564f666ea699d73760a028c01d185713eab801f6",
          "3d80e15784c9b08512d97c7a324cd4b3beb89a5b",
          "64e60a6b2950b8b18084a4e986a378d9b4013751",
          "4e1c81d83229c1710a3256500e159b942787612c",
          "6f9e2c7d5445aa4ed4e8a6add6574aaac043d70e"
        )

        val testKeys_out: Seq[String] = Seq(
          "63156a417e840ba3e6aabc1818df667bb3d2141f",
          "c4e89bccc0434aa3db28f3b835aacc91651868a8"
        )
        val testKeys_err: Seq[String] = Seq(
          "f9a0973ddc768961e4802f8c9d8dd42b603a067c",
          "5f6cfc1a54b793d49d776ab496200e1859b7aa41"
        )

        for (testKey <- testKeys) {
          storage.storeInfo(testKey, dummyInfo)
        }

        for (testKey <- testKeys_out) {
          storage.storeLog(testKey, LogType_Output, dummyLog)
        }
        for (testKey <- testKeys_err) {
          storage.storeLog(testKey, LogType_Error, dummyLog)
        }

        assert(storage.complete("7") ==
          Set("7ac8c4dbd3c61885c40cc3a9c2d02f99b3eaf94b"))

        assert(storage.complete("5") ==
          Set("564f666ea699d73760a028c01d185713eab801f6"))

        assert(storage.complete("6") ==
          Set("64e60a6b2950b8b18084a4e986a378d9b4013751",
            "6f9e2c7d5445aa4ed4e8a6add6574aaac043d70e"
          ))

        assert(storage.complete("6", true) ==
          Set("63156a417e840ba3e6aabc1818df667bb3d2141f"
          ))


      }
    }
  }
}

