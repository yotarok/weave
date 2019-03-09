package ro.yota.weaveschd

import play.api.libs.json.{Writes,JsObject,JsString,JsArray}

sealed trait TaskStatus {
  def name: String;
}
case object TaskStatus_Added extends TaskStatus {
  def name: String = "Added"
}
case object TaskStatus_Pending extends TaskStatus {
  def name: String = "pending"
}
case object TaskStatus_Waiting extends TaskStatus {
  def name: String = "waiting"
}
case object TaskStatus_Running extends TaskStatus {
  def name: String = "running"
}
case object TaskStatus_Completed extends TaskStatus {
  def name: String = "completed"
}
case object TaskStatus_Failed extends TaskStatus {
  def name: String = "failed"
}


//@serializable
case class Task(val id: String,
  val waitFor: Seq[String],
  val script: String,
  val stdoutFile: String,
  val stderrFile: String,
  val shellCommand: String,
  val status: TaskStatus = TaskStatus_Added,
  val retCode: Int = 0) {
}
object Task {
  implicit val implicitTaskWrites = new Writes[Task] {
    def writes(task: Task) = JsObject(Seq(
      "id" -> JsString(task.id),
      "waitFor" -> JsArray(task.waitFor.map(id => JsString(id))),
      "stdoutFile" -> JsString(task.stdoutFile),
      "stderrFile" -> JsString(task.stderrFile),
      "shellCommand" -> JsString(task.shellCommand),
      "script" -> JsString(task.script),
      "status" -> JsString(task.status.name)
    ))
  }
}
