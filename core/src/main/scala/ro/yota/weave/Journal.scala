package ro.yota.weave

import slick.jdbc.SQLiteProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.scalalogging.Logger
import scalax.file.Path

import play.api.libs.json._
import play.api.libs.json.Json._
import scala.concurrent._
import scala.concurrent.duration._
import java.sql.Timestamp
import java.time.Instant

/**
  * Case class representing rows in orders table
  */
case class OrderInfo(
  val id: Option[Long],
  val orderId: String,
  val storageId: String,
  val name: String,
  val digest: String,
  val orderedAt: Timestamp,
  val jars: String);

case class JournalJobInfo(
  val id: Option[Long],
  val orderId: String,
  val digest: String,
  val launchInfo: String,
  val jobInfo: String,
  val scheduledAt: Timestamp
) {
  lazy val launchInfoJson = Json.parse(launchInfo)
  lazy val jobInfoJson = Json.parse(jobInfo)

  def jobId = (jobInfoJson \ "jobId").as[String]
  def outputKey = (launchInfoJson \ "outputKey").as[String]
}


/**
  * Journal database scheme classes for slick
  */
object JournalDatabase {
  // scalastyle:off public.methods.have.type method.name

  class Orders(tag: Tag) extends Table[OrderInfo](tag, "ORDERS") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def orderId = column[String]("ORDER_ID")
    def storageId = column[String]("STORAGE_ID")
    def name = column[String]("NAME")
    def digest = column[String]("DIGEST")
    def orderedAt = column[Timestamp]("ORDERED_AT")
    def jars = column[String]("LOADED_JARS") // Commma-delimited

    def * = (id.?, orderId, storageId, name, digest, orderedAt, jars) <> (
      OrderInfo.tupled, OrderInfo.unapply)
  }
  val orders = TableQuery[Orders]

  class Jobs(tag: Tag) extends Table[JournalJobInfo](tag, "JOBS") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def orderId = column[String]("ORDER")
    def digest = column[String]("DIGEST")
    def launchInfo = column[String]("LAUNCHINFO")
    def jobInfo = column[String]("JOBINFO")
    def scheduledAt = column[Timestamp]("SCHEDULED_AT")

    def * = (id.?, orderId, digest, launchInfo, jobInfo, scheduledAt) <> (
      JournalJobInfo.tupled, JournalJobInfo.unapply)

    def order = foreignKey("ORDER_FK", orderId, orders)(_.orderId)
  }
  val jobs = TableQuery[Jobs]

  def allTables = Seq(orders, jobs)

  def setup =
    DBIO.seq((allTables.map(x => x.schema.create)):_*)
  // scalastyle:on public.methods.have.type method.name

  def verifySchema[A <: Table[_]]
    (db: Database, table: TableQuery[A]): Boolean = {
    try {
      val first =
        Await.result(db.run(table.take(1).result.head),
          Duration.Inf)
      true
    } catch {
      case _: Throwable => {
        false
      }
    }
  }

  def verifyAllSchema(db: Database): Boolean = {
    allTables.forall(tab => verifySchema(db, tab))
  }
}


/**
  * Class for accessing DB containing all previous order history
  */
class Journal(val config: JsObject)(implicit val logger: Logger) {
  private[this] val databasePathStr = (config \ "path").as[String]
  logger.debug(s"Database path = jdbc:sqlite:${databasePathStr}")
  val database = slick.jdbc.H2Profile.api.Database.forURL(
    s"jdbc:sqlite:${databasePathStr}",
    driver = "org.sqlite.JDBC")

  private[this] val databasePath = Path.fromString(databasePathStr)

  // Size is checked for enabling unit test with temporary file
  if (databasePath.exists && databasePath.size.getOrElse(0L) > 0) {
    if (! JournalDatabase.verifyAllSchema(database)) {
      throw new RuntimeException(s"Journal file inconsistency found. Please delete ${databasePathStr} and retry")
    }
  } else {
    logger.info(s"New journal database is initialized at ${databasePathStr}")
    Await.result(database.run(JournalDatabase.setup.transactionally),
      Duration.Inf)
  }

  /**
    * Write order info to the journal
    */
  def writeOrders(id: String, storageId: String,
    varNames: Seq[String], digests: Seq[String], jars: Seq[String]): Unit = {
    val jarsStr = jars.mkString(",")
    val orderTimestamp = Timestamp.from(Instant.now())
    val q = (
      JournalDatabase.orders.map(c =>
        (c.orderId, c.storageId, c.name, c.digest, c.orderedAt, c.jars))
        ++= (
          for ((n, d) <- varNames zip digests) yield {
            (id, storageId, n, d, orderTimestamp, jarsStr)
          }))
    Await.result(database.run(q), Duration.Inf)
  }

  def writeNewJobs(orderId: String,
    digestAndLaunchInfo: Seq[(String, String, String)]): Unit = {
    val now = Timestamp.from(Instant.now())

    val q = JournalDatabase.jobs.map(c =>
      (c.orderId, c.digest, c.launchInfo, c.jobInfo, c.scheduledAt)) ++= (
      for ((digest, launchInfo, jobInfo) <- digestAndLaunchInfo) yield {
        (orderId, digest, launchInfo, jobInfo, now)
      })

    Await.result(database.run(q), Duration.Inf)
  }

  /**
    * Query latest N orders
    */
  def selectLatestOrders(n: Int): Seq[OrderInfo] = {
    import JournalDatabase._;
    val q = orders.sortBy(_.orderedAt.desc).take(n).result
    Await.result(database.run(q), Duration.Inf).toSeq
  }

  /**
    * Get list of JobInfos corresponding to the specified order
    */

  def getJobs(orderId: String): Seq[JournalJobInfo] = {
    import JournalDatabase._;
    val q = jobs.filter(_.orderId === orderId).sortBy(_.id).result
    Await.result(database.run(q), Duration.Inf).toSeq
  }

  /**
    * Get order id of the last order
    */
  def lastOrderId: Option[String] = {
    val seq = selectLatestOrders(1)
    if (seq.size == 0) {
      None
    } else {
      Some(seq.head.orderId)
    }
  }
}

object Journal {
  /**
    * Default logger for journal
    */
  val defaultLogger = Logger("ro.yota.weave.Journal")
  /**
    * Instantiate Journal from the given config json
    *
    * So far, we don't have any alternative implementation for Journal,
    * therefore this function just calls a constructor
    */
  def createJournal(conf: JsObject)(implicit logger: Logger = defaultLogger):
      Journal = {
    new Journal(conf)
  }

}
