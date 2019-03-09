package ro.yota.weave

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}
import play.api.libs.json.JsValue

/**
  * Trait for classes that can be ordered
  */
trait Orderable {
  def subOrders: Seq[(String, Orderable)] = Seq()
  def classFullName: String = {
    val classLoader = this.getClass.getClassLoader
    val mir = ru.runtimeMirror(classLoader)
    val imir = mir.reflect(this)
    imir.symbol.fullName
  }
  def className: String = {
    val classLoader = this.getClass.getClassLoader
    val mir = ru.runtimeMirror(classLoader)
    val imir = mir.reflect(this)
    imir.symbol.name.toString
  }

}


/**
  * This object defines readable names for checkpoint levels
  *
  * Checkpoint levels are used in two ways depending on the context; (a)
  * for specifying the importance of each checkpoint, and (b) for specifying
  * the threshold for checkpoint upload
  */
final object CheckPointLevel {
  /**
    * Upload nothing
    */
  final val Nothing = 0;
  /**
    * Specify that the checkpoint is important
    */
  final val Important = 1;
  /**
    * Specify that the checkpoint is a main result
    */
  final val Result = 1;
  /**
    * Specify that the checkpoint is an intermediate result
    */
  final val IntermediateResult = 3;
  /**
    * Specify that the checkpoint is a processed data
    */
  final val IntermediateData = 7;
  /**
    * Upload every checkpoint
    */
  final val Everything = 10;
};


/**
  * Context info for project evaluation
  *
  *  checkPointPrefix
  *    Root path for check point files, e.g. Seq("/home", "aaa")
  *  traceMessages
  *    Debug messages in a stack
  *  checkPointLevel
  *    Verbosity level for selecting which checkpoint should be activated
  *  configVars
  *    Configurable variables
  */
final case class ProjectContext(
  val checkPointPrefix: Seq[String],
  val checkPointLevel: Int,
  // ^ 0 = nothing, 1 = only important, ..., 10 = all checkpoints
  val configVars: Map[String, JsValue],
  val traceMessages: Seq[String]
)

/**
  * Global object holding stack of project evaluation
  *
  * The stack is mainly operated by member functions of Project
  *
  * Having global status is not a good practice. However, considering that
  * recipe evaluation is single-threaded, and it drastically simplifies
  * the EDSL code, it is okay here.
  */
final object ProjectContextStack {
  final private[this] var instanceStack: List[ProjectContext] = List(initial());

  /**
    * Create default context value
    */
  final def initial(): ProjectContext = {
    ProjectContext(Seq(), 0, Map(), Seq())
  }

  /** Push new status to the stack */
  final def push(nctx: ProjectContext): Unit = {
    instanceStack = nctx :: instanceStack
  }

  /** Pop the status from the stack */
  final def pop(): Unit = {
    instanceStack = instanceStack match {
      case (x :: xs) => xs
      case _ => {
        throw new RuntimeException("ProjectContextStack exhausted");
      }
    }
  }

  /** Return top of the stack */
  final def current(): ProjectContext = {
    instanceStack.head
  }
}


class Project extends Orderable {

  // Here, I'm trying to avoid CRTP like Project[DerivedClass]
  // Since it infects to everywhere and Project writer shouldn't care whether
  // the project will be derived or not.
  // Reflection is a good alternative at a small cost.

  override def subOrders: Seq[(String, Orderable)] = {
    val classLoader = this.getClass.getClassLoader
    val mir = ru.runtimeMirror(classLoader)
    val imir = mir.reflect(this)
    val members = imir.symbol.typeSignature.members

    (for (
      field <- members;
      if field.typeSignature <:< ru.typeOf[Orderable]
    ) yield {
      val orderable =
        imir.reflectField(field.asInstanceOf[ru.TermSymbol])
          .get.asInstanceOf[Orderable]
      (field.name.toString, orderable)
    }).toSeq
  }

  /**
    * Get configuration variable
    *
    * Throw exception if there's no config variable defined
    */
  def configVar(key: String, default: Option[JsValue] = None): JsValue = {
    ProjectContextStack.current().configVars.get(key).orElse(default) match {
      case Some(v) => v
      case None => {
        throw new RuntimeException(s"Config variable ``${key}'' is not defined")
      }
    }
  }

  def setCheckPointLevel(i: Int): Unit = {
    val cur = ProjectContextStack.current()
    val nctx = cur.copy(checkPointLevel = i)
    ProjectContextStack.pop()
    ProjectContextStack.push(nctx)
  }

  /** Overwrite the current checkPointPrefix to the root */
  def setCheckPointRoot(s: String): Unit = {
    val cur = ProjectContextStack.current()
    val nctx = cur.copy(checkPointPrefix = Seq(s))
    ProjectContextStack.pop()
    ProjectContextStack.push(nctx)
  }

  /**
    * push context vars
    */
  def planWith[T](checkPointPath: String = "", trace: String = "")(code: =>T): T = {
    val cur = ProjectContextStack.current()
    var nctx = cur
    if (checkPointPath.length > 0) {
      nctx = nctx.copy(checkPointPrefix = cur.checkPointPrefix ++ Seq(checkPointPath))
    }
    if (trace.length > 0) {
      nctx = nctx.copy(traceMessages = cur.traceMessages ++ Seq(trace))
    }
    ProjectContextStack.push(nctx)
    try {
      code
    } finally {
      ProjectContextStack.pop()
    }
  }

}
