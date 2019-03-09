package ro.yota.weaveschd

case object StartScheduler
case object PingScheduler
case object RemoteAck
//case class AddTask(val task: Task)
case class AddTasks(val tasks: Seq[Task])
case object QueryTasks
case class QueryTasks_Ret(val tasks: Seq[Task])
case object ShutdownScheduler
case object WorkerTick
case object WorkerDeclareAvailability
case class QueryFeasibility(val task: Task)
case object WorkerAck
case object WorkerReject
case class OrderTask(val task: Task)
case class WorkerReportTaskSuccess(val task: Task)
case class WorkerReportTaskFailure(val task: Task, val ret: Int)
