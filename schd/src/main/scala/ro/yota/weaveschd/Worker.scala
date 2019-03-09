package ro.yota.weaveschd

import akka.actor.Actor
import akka.actor.ActorPath
import scala.sys.process.ProcessLogger
import scala.sys.process._
import scala.concurrent.duration._
import scalax.file.ImplicitConversions._
import akka.pattern.ask
import java.io.File
import scalax.file.{Path, FileSystem}

import scala.language.postfixOps

class WorkerActor extends Actor {
  val tickInterval: FiniteDuration = 1000 milliseconds
  import context._

  var tickCanceler =
    context.system.scheduler.schedule(tickInterval, tickInterval,
      self, WorkerTick)

  def doTask(task: Task): Int = {
    val scriptFile = Path.createTempFile()
    scriptFile.write(task.script)

    val stdoutFile = if (task.stdoutFile.length > 0) {
      Path.fromString(task.stdoutFile)
    } else { Path.createTempFile() }
    val stderrFile = if (task.stderrFile.length > 0) {
      Path.fromString(task.stderrFile)
    } else { Path.createTempFile() }

    println("Exec task: " + task.id + " by " + context.self.path)
    var ret = -1;
    for {
      outProcessor <- stdoutFile.outputProcessor
      errProcessor <- stderrFile.outputProcessor
      stdout = outProcessor.asOutput
      stderr = errProcessor.asOutput
    } {
      stderr.write("weaveschd: Do task: " + task.id + " by " + context.self.path + "\n")
      stderr.write("weaveschd: Exec: " + task.script + "\n")

      ret = (task.shellCommand + " " + scriptFile.path) ! ProcessLogger ({
        s => stdout.write(s + "\n")}, {
        s => stderr.write(s + "\n")})
      stderr.write("weaveschd: Done task:" + task.id + " with " + ret + "\n")
    }

    println("Done task:" + task.id + " with " + ret)
    ret
  }

  def receive: PartialFunction[Any, Unit] = {
    case WorkerTick => {
      // declare availability
      context.actorSelection("/user/scheduler") ! WorkerDeclareAvailability
    }

    case OrderTask(task) => {
      // do task
      tickCanceler.cancel()

      val ret = doTask(task)

      if (ret == 0) {
        context.actorSelection("/user/scheduler") ! WorkerReportTaskSuccess(task)
      } else {
        context.actorSelection("/user/scheduler") !
          WorkerReportTaskFailure(task, ret)
      }

      tickCanceler = context.system.scheduler.schedule(tickInterval,
        tickInterval, self, WorkerTick)
    }

    case QueryFeasibility(task) => {
      sender ! WorkerAck
    }
  }
}



