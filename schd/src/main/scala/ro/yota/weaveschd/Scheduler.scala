package ro.yota.weaveschd

import akka.actor.{Actor,ActorSystem,ActorPath,ActorRef,Props}
import akka.kernel.Bootable
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import com.typesafe.config.ConfigFactory
import akka.pattern.ask

import scala.language.postfixOps

class SchedulerActor extends Actor {
  val numCore = sys.env.get("WEAVESCHD_NTHREADS")
    .map(_.toInt).getOrElse(Runtime.getRuntime().availableProcessors())
  val workerActors = (1 to numCore).map(i =>
    context.actorOf(Props[WorkerActor], "worker-%d" format i)
  )

  val tasks: collection.mutable.Map[String, Task] = collection.mutable.Map.empty
  val availableWorkerPaths: collection.mutable.Set[ActorPath] = collection.mutable.Set.empty

  def runnableTask: Option[(String, Task)] = {
    tasks.find { case (id, task) =>
      task.status == TaskStatus_Waiting
    }
  }

  def launchTask(workerPath: ActorPath, task: Task): Unit = {
    tasks -= task.id
    tasks += (task.id -> task.copy(status = TaskStatus_Running))
    context.system.actorSelection(workerPath) ! OrderTask(task)
  }

  def addTasks(newtasks: Seq[Task]): Unit = {
    for (task <- newtasks) {
      println(s"Added task: ${task.id}")
      println(s"      will be launched after: ${task.waitFor}")
      sender ! RemoteAck
      val notcompleted = tasks.filter( { case (k, t) =>
        t.status != TaskStatus_Completed})
      tasks += (task.id -> task.copy(status = if (task.waitFor.length == 0) {
        TaskStatus_Waiting
        } else {
        TaskStatus_Pending
      }, waitFor = task.waitFor))
    }
  }

  def updateStatusAfterTaskCompletion(task: Task): Unit = {
    // Update task status to complete
    tasks -= task.id
    tasks += (task.id -> task.copy(status = TaskStatus_Completed))

    // remove dependency and set status to waiting if there's no dependency remaining
    val keySet = tasks.keySet
    keySet.foreach { id =>
      if (tasks(id).status == TaskStatus_Pending &&
        tasks(id).waitFor.contains(task.id)) {
        val nwait = tasks(id).waitFor.filter(_ != task.id)
        //tasks(id).waitFor - task.id
        val nstatus = if (nwait.length == 0) TaskStatus_Waiting else TaskStatus_Pending
        val ntask = tasks(id).copy(
          waitFor = nwait,
          status = nstatus)
        tasks -= id
        tasks += (id -> ntask)
      }
    }
  }

  def updateStatusAfterTaskFailure(task: Task): Unit = {
    tasks -= task.id
    tasks += (task.id -> task.copy(status = TaskStatus_Failed))
  }

  def findRunnableTaskAndLaunch(): Unit = {
    runnableTask match {
      case Some((id, task)) => launchTask(sender.path, task)
      case None => ()
    }
  }


  def receive: PartialFunction[Any, Unit] = {
    case StartScheduler => {
      println("Starting scheduler")
      sender ! RemoteAck
    }

    case PingScheduler => { sender ! RemoteAck }
    case ShutdownScheduler => {
      println("Shutdown ordered")
      sender ! RemoteAck
      context.system.shutdown()
    }
    case WorkerDeclareAvailability => findRunnableTaskAndLaunch()
    case AddTasks(newtasks) => addTasks(newtasks)
    case WorkerReportTaskSuccess(task) => {
      println("Success: " + task.id)
      updateStatusAfterTaskCompletion(task)
    }
    case WorkerReportTaskFailure(task, retcode) => {
      println("Failure: " + task.id + " retcode=" + retcode)
      updateStatusAfterTaskFailure(task)
    }
    case QueryTasks => {
      val ts = Array((tasks.values.toSeq):_*)
      sender ! QueryTasks_Ret(ts)
    }
    case message: String =>
      println("Received message '%s'" format message)
  }
}

object AkkaSchedulerKernel {
  val remotePort = sys.env.getOrElse("WEAVESCHD_PORT", "22552").toInt

  val serverConf = ConfigFactory.parseString(s"""akka {
  log-dead-letters-during-shutdown = off
  loglevel = "WARNING"
  actor {
    provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
    log-remote-lifecycle-events = off
    enabled-transports = ["akka.remote.netty.tcp"]
    netty.tcp {
      hostname = "127.0.0.1"
      port = ${remotePort}
      message-frame-size =  30000000b
      send-buffer-size =  30000000b
      receive-buffer-size =  30000000b
      maximum-frame-size =  30000000b
    }
 }
}""")
  val clientConf = ConfigFactory.parseString("""
akka {
  log-dead-letters-during-shutdown = off
  loglevel = "WARNING"
  actor {
    provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
    log-remote-lifecycle-events = off
    enabled-transports = ["akka.remote.netty.tcp"]
    netty.tcp {
      hostname = "127.0.0.1"
      port = 0
      message-frame-size =  30000000b
      send-buffer-size =  30000000b
      receive-buffer-size =  30000000b
      maximum-frame-size =  30000000b
    }
 }
}""")

  // intended to be called by clients
  def clientActorSystem: ActorSystem =
    ActorSystem("WeaveAkkaClient", AkkaSchedulerKernel.clientConf)

  // intended to be called by clients
  def remoteSchedulerActor(system: ActorSystem): ActorRef =
    system.actorFor(s"akka.tcp://WeaveAkka@127.0.0.1:${remotePort}/user/scheduler")
}

class AkkaSchedulerKernel {

  val system = (try {
    Some(ActorSystem("WeaveAkka", AkkaSchedulerKernel.serverConf))
  } catch {
    case e: org.jboss.netty.channel.ChannelException => {
      println("Port is already in use!")
      System.exit(1)
      None
    }
  }).get

  def startup(): Unit = {
    implicit val timeout = Timeout(30 seconds)
    Await.result(
      system.actorOf(Props[SchedulerActor], "scheduler") ? StartScheduler,
      timeout.duration)
  }

  def shutdown(): Unit = {
    system.shutdown()
  }
}
