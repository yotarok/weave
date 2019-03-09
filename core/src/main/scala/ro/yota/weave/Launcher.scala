package ro.yota.weave

//import com.twitter.util.Eval
import java.io.File
import scalax.file.Path
import scalax.file.ImplicitConversions._
import play.api.libs.json._
import play.api.libs.json.Json._
import scala.util.matching.Regex

import java.net.JarURLConnection
import java.net.URL
import java.net.URLClassLoader

import ro.yota.weave.storage.{Storage, LogType_Error, LogType_Output}
import com.typesafe.scalalogging.Logger

/**
  * Transaction in storage
  */
case class ObjectStage(
  val serializer: Serializer,
  val objectPool: Map[String, Plannable] = Map(),
  val toBeWritten: Set[(String, JsObject)] = Set(),
  val newCacheLinks: Seq[(String, String)] = Seq())
  (implicit val logger: Logger) {

  /**
    * Write all accumulated change to the associated storage
    */
  def flush(): ObjectStage = {
    flushOutput().registerCache()
  }

  /**
    * Write output objects to the associated storage
    */
  def flushOutput(): ObjectStage = {
    logger.info("Flushing output objects from the stage...")
    for ((key, addInfo) <- toBeWritten) {
      objectPool.get(key) match {
        case Some(obj) => {
          logger.info(s"    Writing ${key}...")
          serializer.storeObject(key, obj, addInfo)
        }
        case None => {
          throw new RuntimeException("Registered output object is not staged")
        }
      }
    }
    logger.info("done.")
    this.copy(toBeWritten = Set())
  }

  /**
    * Write cache links to the associated storage
    */
  def registerCache(): ObjectStage = {
    logger.info("Flushing cache links from the stage...")
    for ((cacheKey, objectKey) <- newCacheLinks) {
      logger.info(s"    ${cacheKey} => ${objectKey}")
      serializer.storage.addCache(cacheKey, objectKey)
    }
    logger.info("done.")
    this.copy(newCacheLinks = Seq())
  }

  /**
    * Load the objects to the stage, and returns loaded objects
    */
  def load(keys: Seq[String]): (Seq[Plannable], ObjectStage) = {
    val vals = keys.map { key =>
      objectPool.getOrElse(key, serializer.loadObject(key))
    }
    (vals, this.copy(objectPool = objectPool ++ keys.zip(vals)))
  }

  /**
    * Register the output object to be written in flushOutput method
    */
  def storeObject(outputKey: String, obj: Plannable, execInfo: JsObject):
      ObjectStage = {
    this.copy(
      objectPool = objectPool + (outputKey -> obj),
      toBeWritten = toBeWritten + (outputKey -> execInfo)
    )
  }

  /**
    * Just add an object to object pool
    */
  def addObject(outputKey: String, obj: Plannable): ObjectStage = {
    this.copy(
      objectPool = objectPool + (outputKey -> obj)
    )
  }

  /**
    * Register the cache link to be written in registerCache method
    */
  def addCacheLink(cacheKey: String, objectKey: String): ObjectStage = {
    this.copy(newCacheLinks = newCacheLinks :+ (cacheKey -> objectKey))
  }
}


/// Class for setting up an environment and launching a procedure
class Launcher(val launcherConfig: JsObject)
  (implicit val logger: Logger = Logger(classOf[Launcher])) {

  def this(configFilePath: Path)
    (implicit logger: Logger) {
    this(Json.parse(configFilePath.string).asInstanceOf[JsObject])
  }

  val storage = Storage.createStorage((launcherConfig \ "storage").as[JsObject])

  val publisher = (launcherConfig \ "publishers").asOpt[JsArray] match {
    case Some(conf) => Publisher.createPublisher(conf)
    case None => Publisher.createDefaultPublisher()
  }


  /**
    * Search cache and return a cached object
    *
    * This function returns None if cached key equals to output key for avoiding
    * circular reference.
    *
    * @return
    *   Tuple of key for cache (if defined), and object for cache (if found)
    */
  def searchCache(serializer: Serializer, func: Function, inputs: Seq[Plannable], outputKey: String):
      (Option[String], Option[Plannable]) = {
    val cacheKeyOpt = func.cacheKey(inputs)
    val cachedObjOpt = for {
      cacheKey <- cacheKeyOpt;
      cachedObjectKey <- serializer.storage.searchCache(cacheKey);
      if cachedObjectKey != outputKey
    } yield {
      logger.info(s"Cache found: Cache=${cacheKey}, Object=${cachedObjectKey}")
      serializer.loadObject(cachedObjectKey)
    }
    (cacheKeyOpt, cachedObjOpt)
  }

  /**
    * Launch single function with given Stage and returns updated Stage
    */
  def launchFunction(initStage: ObjectStage, func: Function,
    inputKeys: Seq[String], outputKey: String, launchInfo: JsObject,
    isAuxiliary: Boolean): ObjectStage = {
    var stage: ObjectStage = initStage;
    if (! stage.serializer.storage.exists(outputKey)) {
      val (inputs, nstage) = stage.load(inputKeys)
      stage = nstage

      val (cacheKey, cacheObj) = searchCache(stage.serializer,
        func, inputs, outputKey)

      val (obj, execInfo) = cacheObj match {
        case Some(obj) => {
          val execInfo = Json.obj(
            "launchInfo" -> launchInfo, "execStat" -> Json.obj(),
            "cacheHit" -> cacheKey)
          (obj, execInfo)
        }
        case None => {
          val (obj, startDate, endDate) = Timer.run {
            func.launch(stage.serializer, inputs)
          }
          val durationMsec = endDate.getTime() - startDate.getTime()
          val execInfo = Json.obj(
            "launchInfo" -> launchInfo,
            "execStat" -> Json.obj(
              "startAt" -> W3CDTF.format(startDate, Some("GMT")),
              "endAt" -> W3CDTF.format(endDate, Some("GMT")),
              "durationMsec" -> durationMsec)
          )
          logger.info(s"main executed in ${durationMsec} msec" )
          (obj, execInfo)
        }
      }

      if (! obj.isValid()) {
        throw new RuntimeException("Failed output validation")
      }

      if (isAuxiliary) {
        stage = stage.addObject(outputKey, obj)
      } else {
        stage = stage.storeObject(outputKey, obj, execInfo)
        if (! cacheKey.isEmpty) {
          stage = stage.addCacheLink(cacheKey.get, outputKey)
        }
      }

      val checkPointInfo =
        (launchInfo \ "checkPoints").
          asOpt[Seq[JsArray]].getOrElse(Seq()).map {
            arr => (
              arr(0).as[String],
              arr(1).as[String].split('.').dropWhile(_.length == 0)
            )
          }
      if (! checkPointInfo.isEmpty) {
        obj.outputKey = outputKey
      }
      for ((loc, fields) <- checkPointInfo) {
        logger.info(
            s"Trying to publish ${fields.mkString(",")} to checkpoint at ${loc}")
        stage.serializer.exportObject(
          publisher,
          loc, obj.field(fields))
      }
      stage
    } else {
      logger.info(s"Object ${outputKey} already exists, skip.")
      stage
    }
  }

  def launch(launchInfoPath : Path) {
    val launchInfo = Json.parse(launchInfoPath.string).asInstanceOf[JsObject]

    // Logging is now moved to outside of JVM, as there's no handy way to
    // capture stdout/err including subprocess' ones

    println("""
===========
launch info
===========
""" + Json.prettyPrint(JsonOps.hideSecretInfo("^_.*$".r, launchInfo)))

    val serializer = new Serializer(storage, (launchInfo \ "jarKeys").as[Seq[String]])

    var stage = ObjectStage(serializer)

    val launchType = (launchInfo \ "type").asOpt[String].getOrElse("")
    launchType match {
      case "Procedure" => {
        val className = (launchInfo \ "functionClassName").as[String]
        val inputKeys = (launchInfo \ "inputKeys").as[Seq[String]]
        val outputKey = (launchInfo \ "outputKey").as[String]

        val func = Eval.evalMany[Function](serializer.classLoader, Seq("import ro.yota.weave._ ; new " + className + "()")).head

        Timer.profile {
          d=>logger.info(s"Function completed in ${d} msec" )
        } {
          stage = launchFunction(stage, func, inputKeys, outputKey, launchInfo, false)
        }
      }
      case "CollapsedProcedure" => {
        val finalOutputKey = (launchInfo \ "outputKey").as[String]
        for (childLaunchInfo <- (launchInfo \ "elements").as[Seq[JsObject]]) {
          val className = (childLaunchInfo \ "functionClassName").as[String]
          val inputKeys = (childLaunchInfo \ "inputKeys").as[Seq[String]]
          val outputKey = (childLaunchInfo \ "outputKey").as[String]

          val func = Eval.evalMany[Function](serializer.classLoader, Seq("import ro.yota.weave._ ; new " + className + "()")).head

          Timer.profile {
            d=>logger.info(s"Function completed in ${d} msec" )
          } {
            stage = launchFunction(stage, func, inputKeys,
              outputKey, childLaunchInfo, outputKey != finalOutputKey)
          }
        }
      }
      case _ => {
        throw new RuntimeException(s"Unknown launchType: ${launchType}")
      }
    }
    stage.flush()

  }
}
