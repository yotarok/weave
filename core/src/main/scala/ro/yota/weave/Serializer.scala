package ro.yota.weave

import scalax.file.Path
import scalax.file.ImplicitConversions._
import play.api.libs.json._
import play.api.libs.json.Json._

import ro.yota.weave.{planned => p}
import ro.yota.weave.storage.Storage
import scalax.io.{Input, Resource}

/**
  * A bridging class between storage and launcher
  *
  * This class loads files from storage and constructs objects
  */
class Serializer(val storage: Storage, val classLoader: ClassLoader) {

  def this(storage: Storage, jarKeys: Seq[String]) {
    this(storage,
      Eval.classLoaderWithJarPaths(
        jarKeys.map { jarKey =>
          storage.loadJar(jarKey)
        }
      ))
  }

  /** This utility class is used for getting class from a class loader
    *
    * Since Java class loader does not support type arguments,
    * arguments needs to be removed before used for deserialization
    */
  private[this] def removeTypeParameters(s: String): String = {
    val rexp = (raw"\[[^\]]+\]".r);
    rexp.replaceAllIn(s, "")
  }


  /**
    * Load primitive object with the given path and the given info
    */
  def loadPrimitiveObject(path : Path, info: JsObject)
      : Plannable = {
    val klassName = (info \ "className").as[String]

    val klass = classLoader.loadClass(removeTypeParameters(klassName))

    val ret = if (classOf[p.File].isAssignableFrom(klass)) {
      // if the object is a file
      val ctr = klass.getConstructor(classOf[Path])
      // instantiate with a path
      ctr.newInstance(path).asInstanceOf[Plannable]
    } else { // Or else, call a constructor with Input object
      val f = Eval.eval[(ClassLoader, Input) => Plannable](
            classLoader, s"""
(cl: ClassLoader, inp: scalax.io.Input) => {
  new ${klassName}(cl, inp)
}""")
      f(classLoader, Resource.fromFile(path.fileOption.get))
    }

    ret.loadedContentDigest = (info \ "contentDigest").asOpt[String]
    ret
  }

  /**
    * Load primitive object with the given key and the given info
    */
  def loadPrimitiveObject(key: String, info: JsObject)
      : Plannable = {
    val destpath = Path.createTempFile()

    storage.loadObjectBody(key, destpath)
    // If input is File, instantiate directly with path
    val ret = loadPrimitiveObject(destpath, info)
    ret.objectKey = Some(key)
    ret
  }


  /**
    * Load object with the given key and the given info
    */
  def loadObject(key: String, originalInfo: JsObject): Plannable = {
    val info = storage.loadInfo(key);
    (info \ "objectRef").asOpt[String] match {
      case Some(linkKey) if key != linkKey => {
        this.loadObject(linkKey, originalInfo)
      }
      case _ => {
        if ((originalInfo \ "isPrimitive").as[Boolean]) {
          loadPrimitiveObject(key, originalInfo)
        } else {
          val src = (originalInfo \ "fieldNames").as[Seq[String]].map { field =>
            (field -> loadObject(Hasher(key + "." + field)))
          }.toMap
          val klassName = (originalInfo \ "className").as[String]

          import scala.collection.immutable.Map;

          val f = Eval.eval[(Map[String, Plannable]) => Plannable](
            classLoader, s"""
(src: scala.collection.immutable.Map[String, ro.yota.weave.Plannable]) => {
  new ${klassName}(src)
}""")
          val ret = f(src);

          ret.objectKey = Some(key)

          // If the loaded object has contentDigest, copy this
          ret.loadedContentDigest = (info \ "contentDigest").asOpt[String]

          ret
        }
      }
    }
  }

  /**
    * Load object with the given key from the storage
    */
  def loadObject(key: String): Plannable = {
    this.loadObject(key, storage.loadInfo(key))
  }


  def storePrimitiveObject(key: String, obj: Plannable, addInfo: JsObject) {
    if (! obj.objectKey.isEmpty) {
      println(s"Trying to link ${key} to ${obj.objectKey.get}...")
      val linkInfo = Json.obj(
        "objectRef" -> obj.objectKey.get
      )
      val basicInfo = obj.basicInfo
      storage.storeInfo(key, linkInfo ++ basicInfo ++ addInfo)
    } else {
      println(s"Trying to store ${key} as ${obj.className}...")
      val path = obj match {
        case file: p.File => {
          file.path
        }
        case dir: p.Directory => {
          dir.archivePath
        }
        case _ => {
          val tmppath = Path.createTempFile()
          obj.write(tmppath)
          tmppath
        }
      }
      storage.storeObjectBody(key, path)
      storage.storeInfo(key,
        obj.basicInfo ++ addInfo ++ Json.obj("size" -> path.size))
    }
  }

  /**
    * Store object with the given key
    */
  def storeObject(key: String, obj: Plannable, addInfo: JsObject) {
    if (obj.isPrimitive) {
      storePrimitiveObject(key, obj, addInfo)
    } else {
      println(s"Trying to store ${key} as ${obj.className}...")
      storage.storeInfo(key, obj.basicInfo ++ addInfo)

      for (field <- obj.fieldNames) {
        val parentInfo = Json.obj("ownedBy" -> key)
        this.storeObject(Hasher(key + "." + field),
          obj.field(field), parentInfo)
      }
    }
  }

  /** Load published object */
  def importObject(publisher: Publisher, loc: String): Plannable = {
    val infoPath = publisher.retrieve(loc + ".info") match {
      case None => throw new RuntimeException("Object [info] is not found in " + loc)
      case Some(x) => x
    }
    val info = Json.parse(infoPath.string).as[JsObject]

    if ((info \ "isPrimitive").as[Boolean]) {
      publisher.retrieve(loc) match {
        case Some(path) => loadPrimitiveObject(path, info)
        case None => throw new RuntimeException("Object [body] is not found in " + loc)
      }
    } else {
      val src = (info \ "fieldNames").as[Seq[String]].map { field =>
        (field -> importObject(publisher, loc + "/" + field))
      }.toMap

      val klassName = classLoader.loadClass((info \ "className").as[String])
      val f = Eval.eval[(Map[String, Plannable]) => Plannable](
        classLoader, s"""
(src: scala.collection.immutable.Map[String, ro.yota.weave.Plannable]) => {
  new ${klassName}(src)
}""")
      f(src);
    }
  }

  /** Store object to publiser */
  def exportObject(publisher: Publisher, loc: String, obj: Plannable) {
    val infopath = Path.createTempFile()
    val infoobj = obj.basicInfo ++ {
      obj.outputKey match {
        case Some(key) => Json.obj("exportedFrom" -> key)
        case None => Json.obj()
      }
    }

    infopath.write(Json.stringify(infoobj))
    publisher.publish(loc + ".info", infopath)

    if (obj.isPrimitive) {
      val datapath = Path.createTempFile()
      for {
        processor <- datapath.outputProcessor
      } {
        obj.write(processor.asOutput)
      }
      publisher.publish(loc, datapath)
    } else {
      for (key <- obj.fieldNames) {
        exportObject(publisher, loc + "/" + key, obj.field(key))
      }
    }
  }

}

