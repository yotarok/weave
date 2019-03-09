package ro.yota.weave

import java.util.{Calendar, Date, TimeZone}

import scala.util.parsing.combinator._
import scala.util.parsing.input._
import scalax.file.Path
import java.net.URL
import java.net.URLClassLoader
import scala.reflect.runtime.{universe => ru}

import scala.reflect.runtime.universe
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object Eval {
  /** Utility function for creating ClassLoader from the given paths to jar files
    */
  def classLoaderWithJarPaths(jarPaths: Seq[Path]) : ClassLoader = {
    val urls = jarPaths.map({ (path: Path) =>
      new URL(s"jar:file:${path.toAbsolute.path}!/")
    }).toArray

    URLClassLoader.newInstance(urls, this.getClass.getClassLoader())
  }


  /** Evaluate a scala statement
    */
  def eval[T](cl: ClassLoader, source: String) : T = {
    val mir = universe.runtimeMirror(cl)
    val toolbox = mir.mkToolBox()
    val tree = toolbox.parse(source)
    toolbox.eval(tree).asInstanceOf[T]
  }

  /** Evaluate scala statements in the same toolbox
    */
  def evalMany[T](cl: ClassLoader, sources: Seq[String]) : Seq[T] = {
    val mir = universe.runtimeMirror(cl)
    val toolbox = mir.mkToolBox()
    sources.map { src =>
      val tree = toolbox.parse(src)
      toolbox.eval(tree).asInstanceOf[T]
    }
  }
}


object TermUI {
  def termWidth: Int = jline.TerminalFactory.get().getWidth()

  /**
    * Show prompt and ask to choose one from choices
    *
    * The first choice is treated as a default value. If the user doesn't
    *  answer, or the command is not running on an interactive shell, this
    *  function returns the default value.
    */
  def prompt(
    msg: String,
    choices: Seq[String]): String = {
    if (TermUI.isTTY) {
      choices.head
    } else {
      val reader = new jline.console.ConsoleReader()
      val choiceMsg = "(" + choices.mkString("/") + ")"
      print(s"${msg} ${choiceMsg} ?")
      var answer: Option[String] = None
      while (answer.isEmpty) {
        val ans = reader.readLine().trim
        if (choices contains ans) {
          answer = Some(ans)
        }
        print(s"  ${choiceMsg} ?")
      }
      answer.get
    }
  }

  val isTTY: Boolean = (System.console() != null) // scalastyle:ignore
}


object JsonOps {
  import play.api.libs.json.{JsObject, JsArray, JsValue, JsString}
  import scala.util.matching.Regex

  def hideSecretInfoObject(keyRex: Regex, obj: JsObject, rep: String): JsObject =
    JsObject(for ((k, v) <- obj.fields) yield {
      k match {
        case keyRex(_*) => {
          (k -> JsString(rep))
        }
        case _ => {
          (k -> hideSecretInfo(keyRex, v, rep))
        }
      }
    })

  /**
    * Hide keys matching with the specified regex
    *
    *  Key search is done recursively, the value will be replaced to rep
    */
  def hideSecretInfo(keyRex: Regex, v: JsValue, rep: String = "__hidden__"): JsValue = {
    v match {
      case obj: JsObject => hideSecretInfoObject(keyRex, obj, rep)
      case arr: JsArray => JsArray(arr.value.map(hideSecretInfo(keyRex, _, rep)))
      case _ => v
    }
  }

}


object Prettifier {
  def truncate(s: String, width: Int): String = {
    if (s.length <= width) {
      s
    } else {
      if (width <= 5) {
        throw new RuntimeException("Cannot truncate the width less than 5")
      }
      s.take(width - 5) + " ... "
    }
  }

  /**
    * Make class name short by omitting package names
    */
  def shortenClassName(name: String): String = {
    case object ClassNameParser extends RegexParsers {
      override type Elem = Char
      override val skipWhitespace = true

      def identifier = raw"[a-zA-Z0-9\.]+".r

      def className: Parser[String] =
        identifier ~ ("[" ~ repsep(className, ",") ~ "]").? ^^ {
          case (kls ~ None) => kls.split(raw"\.").last
          case (kls ~ Some(_ ~ lst ~ _)) =>
            kls.split(raw"\.").last + "[" + lst.map({ s =>
              s.split(raw"\.").last
            }).mkString(",") +"]"
        }
    }
    ClassNameParser.parseAll(ClassNameParser.className, name) match {
      case ClassNameParser.Success(res, _) => res
      case _ => {
        name
      }
    }
  }

  def prettifyFunctionSignature(sign: String): String = {
    val functionRexp = raw"^([^(]*)\(([a-zA-Z0-9,]*)\).*$$".r
    val className = functionRexp.replaceFirstIn(sign, raw"$$1")
    val argList = functionRexp.replaceFirstIn(sign, raw"$$2")
    val shortArgList = argList.split(",").map(_.take(8)).mkString(",")
    val shortClassName = shortenClassName(className)
    s"${shortClassName}(${shortArgList})"
  }
}

/**
  * Utility object for handling W3CDTF
  */
object W3CDTF {
  /**
    * Format java.util.Date object to W3CDTF string
    */
  def format(date: Date, timezone: Option[String]): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    timezone match {
      case Some(tzname) => cal.setTimeZone(TimeZone.getTimeZone(tzname))
      case None => {}
    }
    val y = cal.get(Calendar.YEAR)
    val m = cal.get(Calendar.MONTH) + 1
    val d = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val sec = cal.get(Calendar.SECOND)
    val msec = cal.get(Calendar.MILLISECOND)
    val offset_min =
      (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / 60000;

    val tzstr = if (offset_min == 0) {
      "Z"
    } else {
      val (sign, abs) = if (offset_min > 0) {
        ("+", offset_min)
      } else {
        ("-", - offset_min)
      }
      val offset_hour = (abs / 60);
      val offset_moh = (abs % 60);
      f"$sign%s$offset_hour%02d:$offset_moh%02d"
    }
    f"$y%04d-$m%02d-$d%02dT$hour%02d:$min%02d:$sec%02d.$msec%03d$tzstr%s"
  }
}

trait SpinnerBase {
  def start(): Unit
  def stop(reportTime: Boolean = true): Unit
}

case class SpinnerStyle(
  val spin: Seq[String],
  val duration: Long,
  val formatter: (String, String) => String
)

/**
  * Class for showing fancy spinner in the terminal
  *
  * This can be viewed as CLI-friendly version of Timer
  *
  * @param header Header message before the spinner
  * @param spin As (a, b), Spinner content where a is each string in an
  *            animation, and b is duration of each frame in msec
  * @param footer Footer message after the spinner
  */
class Spinner(style: SpinnerStyle, msg: String) extends SpinnerBase {

  class SpinnerMain extends Runnable {
    var alive = true;
    var startTime: Long = 0
    var cur = 0
    def run() {
      while (alive) {
        print("\u001b[1K\r" + style.formatter(style.spin(cur), msg))
        cur = (cur + 1) % style.spin.length
        Thread.sleep(style.duration)
      }
      println("")
    }
  }

  private[this] val mainFunc = new SpinnerMain()
  private[this] val thread = new Thread(mainFunc)

  def start(): Unit = {
    mainFunc.startTime = System.currentTimeMillis
    thread.start
  }
  def stop(reportTime: Boolean = true): Unit = {
    mainFunc.alive = false
    val lastmsg = if (reportTime) {
      val now = System.currentTimeMillis
      val dur = now - mainFunc.startTime
      msg + s" done [${dur} msec]\n"
    } else {
      msg + "\n"
    }
    print("\u001b[1K\r" + style.formatter(style.spin(mainFunc.cur), lastmsg))
  }
}

object Spinner {
  class DummySpinner(msg: String) extends SpinnerBase {
    var startTime: Long = 0;
    def start(): Unit = {
      startTime = System.currentTimeMillis
      print(msg + " ... ")
    }
    def stop(reportTime: Boolean = true): Unit = {
      if (reportTime) {
        val now = System.currentTimeMillis
        val dur = now - startTime
        println(s" ..done [${dur} msec]\n")
      } else {
        println("")
      }
    }
  }

  /**
    * Show spinner while executing f
    */
  def withSpinning[A](
    msg: String,
    style: SpinnerStyle = Spinner.preferredStyle,
    reportTime: Boolean = true)(f: =>A): A = {

    val spinner = if (TermUI.isTTY) {
      new Spinner(style, msg)
    } else {
      new DummySpinner(msg)
    }

    spinner.start()
    try {
      val ret = f
      ret
    } finally {
      spinner.stop(reportTime)
    }
  }

  //scalastyle:off magic.number
  /**
    * Clock style
    */
  val clock = SpinnerStyle(
    Seq(
      "\ud83d\udd50", "\ud83d\udd51", "\ud83d\udd52", "\ud83d\udd53",
      "\ud83d\udd54", "\ud83d\udd55", "\ud83d\udd56", "\ud83d\udd57",
      "\ud83d\udd58", "\ud83d\udd59", "\ud83d\udd5a", "\ud83d\udd5b"
    ), 100L, (spin, msg) => {
      s"  ${spin}  ${msg}"
    })

  /**
    * Blinker (white) style
    */
  val blinkerBlackWhite = SpinnerStyle(
    Seq(
      "\u26aa ", "\u26ab "
    ), 200L, (spin, msg) => {
      s"  ${spin}  ${msg}"
    })

  val blinkerRedBlue = SpinnerStyle(
    Seq(
      "\ud83d\udd34 \ud83d\udd35 \ud83d\udd35 ",
      "\ud83d\udd35 \ud83d\udd34 \ud83d\udd35 ",
      "\ud83d\udd35 \ud83d\udd35 \ud83d\udd34 "
    ), 200L, (spin, msg) => {
      s"  ${spin}  ${msg}"
    })

  /**
    * Bar style (ASCII compatible)
    */
  val asciiBars = SpinnerStyle(
    Seq(
      "[|]", "[/]", "[-]", "[\\]"
    ), 300L, (spin, msg) => {
      s"  ${spin}  ${msg}"
    })
  //scalastyle:on magic.number

  /**
    * Select the fanciest style depending on the terminal setting
    */
  lazy val preferredStyle = {
    if ((sys.env contains "TERM_PROGRAM") && TermUI.isTTY) {
      asciiBars
    } else {
      asciiBars
    }
  }
}

object Timer {
  /**
    * Run something and compute difference between ending time and starting time
    *
    * This can be viewed as log-griendly version of Spinner
    */
  def run[A](f: =>A): (A, Date, Date) = {
    val start = new Date()
    val ret: A = f
    val end = new Date()
    (ret, start, end)
  }


  def profile[A](show: Double=>Unit)(f: =>A): A = {
    val (ret, beg, end) = run(f)
    val dur: Double = end.getTime() - beg.getTime()
    show(dur)
    ret
  }

}

