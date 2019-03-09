package exp

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import sys.process._
import ro.yota.weave.{doclib => doc}
import play.api.libs.json.JsArray

import ro.yota.weave.macrodef._
import ro.yota.weave.macrodef.Lambda._
import scala.language.postfixOps
import scalax.file.Path

@WeaveFunction class MapSymbolsToInt {
  def apply(vocab: p.TextFile, unkSymbol: p.String, text: p.TextFile)
      : p.TextFile = {

    val vocabMap = (for (line <- vocab.path.lines()) yield {
      val vs = line.trim.split(raw"\s+")
      (vs(0) -> vs(1))
    }).toSeq.toMap

    val output = p.TextFile.output()
    output.path.writeStrings((for (line <- text.path.lines()) yield {
      (for (word <- line.trim.split(raw"\s+")) yield {
        vocabMap.get(word).orElse(vocabMap.get(unkSymbol.value)) match {
          case Some(v) => v
          case None => {
            throw new RuntimeException("Unknown symbol ${unkSymbol.value} is not in the vocabulary")
          }
        }
      }).mkString(" ")
    }), "\n")
    output
  }
}

/** Extract symbols for RNNs */
@WeaveFunction class ExtractSymbols {

  def apply(unkSymbol: p.String, cutoff: p.Int, text: p.TextFile)
      : p.TextFile = {
    import collection.mutable.HashMap
    val counts = new HashMap[String,Int]()  {
      override def default(key:String) = 0
    }

    for (line <- text.path.lines()) {
      for {
        word <- line.trim.split(" ");
        if word.nonEmpty
      } {
        counts(word) = counts(word) + 1
      }
    }

    val output = p.TextFile.output()
    output.openAndWrite { f =>
      f.write(s"</s>\t0\n")
      f.write(s"${unkSymbol.value}\t1\n")
      var wordid = 2;
      var keys = counts.keys.toSeq.sorted;
      for (word <- keys) {
        if (word != unkSymbol.value && counts(word) > cutoff.value) {
          f.write(s"${word}\t${wordid}\n")
          wordid += 1
        }
      }
    }
    output
  }
}

@WeaveRecord class RNNLMShape(
  val nOutput: p.Int,
  val nEmbed: p.Int,
  val nHidden: p.Int,
  val nLayer: p.Int,
  val dropOutRate: p.BigDecimal,
  val tiedWeights: p.Boolean
);

@WeaveRecord class RNNLMTrainingParameter(
  val batchSize: p.Int,
  val optimizer: p.String,
  val learnRate: p.BigDecimal,
  val gradientClip: p.BigDecimal
);

@WeaveFunction class DecayLearningRate {
  def apply(
    decayFactor: p.BigDecimal,
    trainParam: RNNLMTrainingParameter): RNNLMTrainingParameter = {
    new RNNLMTrainingParameter(
      trainParam.batchSize,
      trainParam.optimizer,
      trainParam.learnRate.value * decayFactor.value,
      trainParam.gradientClip
    )
  }
}


@WeaveFunction class MakeRNNLMGraph {
  override def version = "2018-05-07"
  def apply(shape: RNNLMShape, bucketKeys: p.String)
      : JsonFile = {
    val output = JsonFile.output()
    val tiedFlag = if (shape.tiedWeights.value) { "--tied" } else { "" }
    val scripts = ResourceUtils.extract(getClass, "/scripts/")
    cmd"""
python3 -u ${scripts}/define_rnnlm.py
    --vocabsize ${shape.nOutput} --dimembed ${shape.nEmbed}
    --dimhidden ${shape.nHidden} --nrnnlayers ${shape.nLayer}
    --dropout ${shape.dropOutRate} --bucketlen ${bucketKeys}
    ${tiedFlag} --output ${output}
    """.exec()

    output
  }
}

@WeaveFunction class InitializeRNNLM {
  override def version = "2018-03-10"
  def apply(
    seed: p.Int,
    symbols: JsonFile)
      : p.File = {
    val output = p.File.output()
    val scripts = ResourceUtils.extract(getClass, "/scripts/")
    cmd"""
python3 -u ${scripts}/initialize_rnnlm.py
    --seed ${seed} --inputsym ${symbols} --output ${output}
""".exec()
    output
  }
}


@WeaveFunction class TrainRNNLM {
  override def version = "2018-05-07"
  def apply(
    trainParam: RNNLMTrainingParameter,
    trainData: p.TextFile,
    contextLength: p.Int,
    seed: p.Int,
    symbols: JsonFile,
    modelParam: p.File): p.File = {
    val output = p.File.output()
    val scripts = ResourceUtils.extract(getClass, "/scripts/")
    cmd"""
python3 -u ${scripts}/train_rnnlm.py
    --inputsym ${symbols} --inputparam ${modelParam}
    --seed ${seed} --contextlen ${contextLength}
    --traindata ${trainData} --batchsize ${trainParam.batchSize}
    --clip ${trainParam.gradientClip}
    --learnrate ${trainParam.learnRate} --output ${output}
""".exec()
    output
  }
}

@WeaveFunction class EvalRNNLM {
  override def version = "2018-02-23"
  def apply(
    evalData: p.TextFile,
    contextLength: p.Int,
    symbols: JsonFile,
    modelParam: p.File): p.BigDecimal = {
    val scripts = ResourceUtils.extract(
      getClass, "/scripts/")

    val result = p.TextFile.temporary()

    val cmd = Seq("python3", "-u",
      scripts.path + "/eval_rnnlm.py",
      "--inputsym", symbols.path.path,
      "--inputparam", modelParam.path.path,
      "--contextlen", contextLength.value.toString,
      "--evaldata", evalData.path.path)
    ProcessUtils.execute(cmd #> result.asFile)

    var totalLStr = ""
    val pattern = "Perplexity: ([0-9.]+)$".r
    for (l <- result.path.lines()) {
      pattern.findAllIn(l).matchData.foreach { m =>
        totalLStr = m.group(1)
      }
    }
    println(s"Found pattern: ${totalLStr}")
    p.BigDecimal(totalLStr)
  }
}

case class RNNLMTraining(
  val netSymbols: Plan[JsonFile],
  var trainParam: Plan[RNNLMTrainingParameter],
  val trainData: Plan[p.TextFile],
  val xvalData: Plan[p.TextFile],
  val numIteration: Int = 30,
  val seedOffset: Int = 0,
  val contextLength: Int = 1,
  val learnRateDecay: Option[Plan[p.BigDecimal]] = None
) extends Project {
  val initParam = InitializeRNNLM(seedOffset, netSymbols)

  var trainedParam = initParam
  val allParams = for (ite <- (0 until numIteration)) yield {
    planWith(trace=s"RNNLM training ite=${ite}") {
      val param = TrainRNNLM(
        trainParam,
        trainData,
        contextLength,
        seedOffset + ite,
        netSymbols, trainedParam)

      learnRateDecay match {
        case Some(v) => {
          trainParam = DecayLearningRate(v, trainParam)
        }
        case None => { }
      }

      trainedParam = param
      param
    }
  }

  val allLogs = for (param <- allParams) yield {
    param.stderr
  }

  val trainingLog = ConcatTextFiles(allLogs)

  val xvalResults = for (param <- allParams) yield {
    EvalRNNLM(xvalData, contextLength, netSymbols, param)
  }

  val bestParam = PickMinimizer(xvalResults, allParams)
  val bestXvalResult = PickMinimizer(xvalResults, xvalResults)
}

object TwitterCharLM extends Project {
  val timestamp = "%tF".format(new java.util.Date())
  val dir = configVar("TWITTER_ARCHIVE_ROOT").as[String]

  val paths =  Path.fromString(dir).children().toSeq.sorted
  val monthlyJs = (for (path <- paths) yield {
    DropHead(1, injectFile(path).as[p.TextFile]).as[JsonFile]
  })

  var monthlyText = for (json <- monthlyJs) yield {
    TableToText("",
      MapColumn(
        lambda("replace line breaks", {(x: String) =>
          x.replaceAll("\n", "<br>")}),
        0, GetCSVFromJson(
          QueryJson(".[] | [.text]", json))))
  }


  val urlPattern = raw"((?:https?|ftp):\/\/[^\s　]+)"
  val urlTag = "<url>"
  val spaceTag = "<spc>"
  val botMarkerPattern = raw"^\[bot\]"
  val userNamePattern = raw"@[_0-9a-zA-Z]"
  val emptyPattern = raw"^\s*$$"

  monthlyText = for ((text, path) <- monthlyText zip paths) yield {
    planWith(trace="Filtering " + path.path) {
      text
        .filterByRegex(userNamePattern, true)
        .filterByRegex(botMarkerPattern, true)
        .filterByRegex(emptyPattern, true)
    }
  }

  monthlyText = for ((text, path) <- monthlyText zip paths) yield {
    planWith(trace="Replacing tags in " + path.path) {
      text
        .substituteRegex(urlPattern, urlTag)
        .substituteRegex(raw" ", spaceTag)
    }
  }

  val chars = for ((text, path) <- monthlyText zip paths) yield {
    planWith(trace="Splitting characters in " + path.path) {
      text.substituteRegex(raw"(<[^>]+>|[^<])", "$1 ")
    }
  }

  val additionalChars =
    for (pathStr <- configVar("ADDITIONAL_TEXT", Some(JsArray())).as[JsArray].value) yield {
      injectFile(Path.fromString(pathStr.as[String])).as[p.TextFile]
        .substituteRegex(urlPattern, urlTag)
        .substituteRegex(raw" ", spaceTag)
        .substituteRegex(raw"(<[^>]+>|[^<])", "$1 ")
    }

  val trainChars = chars.slice(0, chars.size * 19 / 20) ++ additionalChars
  val xvalChars = chars.slice(chars.size * 19 / 20, chars.size - 1) // 5% is xval
  val evalChars = chars.slice(chars.size - 1, chars.size) // the last month is eval

  val trainText = ConcatTextFiles(WeaveArray.fromSeq(trainChars))
  val xvalText = ConcatTextFiles(WeaveArray.fromSeq(xvalChars))
  val evalText = ConcatTextFiles(WeaveArray.fromSeq(evalChars))

  val vocab = ExtractSymbols("<unk>", 1, trainText)
    .checkPoint("twitter/vocab.txt")

  val trainInts = MapSymbolsToInt(vocab, "<unk>", trainText)
    .checkPoint("twitter/int_data_train.txt")
  val xvalInts = MapSymbolsToInt(vocab, "<unk>", xvalText)
    .checkPoint("twitter/int_data_xval.txt")
  val evallInts = MapSymbolsToInt(vocab, "<unk>", evalText)
    .checkPoint("twitter/int_data_eval.txt")

  val learnRate = Constant(p.BigDecimal("0.001")) // 30 ite tested

  val gradientClip = Constant(p.BigDecimal("5.0"))

  val shape = RNNLMShape(CountLines(vocab), 512, 512, 2, Constant(p.BigDecimal("0.5")), false)
  val trainParam = RNNLMTrainingParameter(30, "adam", learnRate, gradientClip)

  val contextLength = 3
  val f:(Int, Int) => List[Int] = {(cur: Int, max: Int) =>
    if (cur < max) {
      cur :: f(cur * 2, max)
    } else {
      cur :: Nil
    }
  }
  val buckets = f(10, 200 * contextLength).mkString(",")
  val netSymbols = MakeRNNLMGraph(shape, buckets)
    .checkPoint("twitter/symbol.json")
    .checkPoint(s"twitter/archive/${timestamp}/symbol.json")

  val train = RNNLMTraining(netSymbols, trainParam, trainInts, xvalInts,
    contextLength = contextLength)

  val bestParam = train.bestParam
    .checkPoint("twitter/parameter.bin")
    .checkPoint(s"twitter/archive/${timestamp}/parameter.bin")


  var plotData = doc.LinePlotData()
  plotData = doc.AddYValuesToLinePlot(plotData, train.xvalResults, "Model", "")
  val lossPlot = doc.DrawLinePlotPNG(plotData, doc.LinePlotFigStyle())
    .checkPoint("twitter/training_plot.png")
    .checkPoint(s"twitter/archive/${timestamp}/training_plot.png")

  val configFile = s"""
{
    "type": "symbol_v1",
    "vocab": "vocab.txt",
    "symbol": "symbol.json",
    "parameter": "parameter.bin",
    "input_name": "data",
    "contextlen": ${contextLength},
    "output_name": "softmax_label",
    "invtemp": 2.0
}""".as[JsonFile]

  val modelPack = CreateDirectory()
    .addFile("symbol.json", netSymbols)
    .addFile("parameter.bin", bestParam)
    .addFile("vocab.txt", vocab)
    .addFile("config.json", configFile)
    .checkPoint("twitter/package.tar")
    .checkPoint(s"twitter/archive/${timestamp}/package.tar")
}
