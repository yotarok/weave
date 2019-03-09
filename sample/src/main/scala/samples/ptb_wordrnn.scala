package exp

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.{doclib => doc}

object PTBWordLM extends Project {
  val timestamp = "%tF".format(new java.util.Date())

  val trainText = ImportFile(
    "https://raw.githubusercontent.com/dmlc/web-data/master/mxnet/ptb/ptb.train.txt"
  ).as[p.TextFile]
  val xvalText = ImportFile(
    "https://raw.githubusercontent.com/dmlc/web-data/master/mxnet/ptb/ptb.valid.txt"
  ).as[p.TextFile]
  val evalText = ImportFile(
    "https://raw.githubusercontent.com/dmlc/web-data/master/mxnet/ptb/ptb.test.txt"
  ).as[p.TextFile]
  val tinyShakespeare = ImportFile(
    "https://raw.githubusercontent.com/dmlc/web-data/master/mxnet/tinyshakespeare/input.txt"
  ).as[p.TextFile]

  val vocab = ExtractSymbols("<unk>", 0, trainText)
    .checkPoint("ptbrnn/vocab.txt")

  val shape = RNNLMShape(CountLines(vocab), 650, 650, 2, Constant(p.BigDecimal("0.5")), false)

  val trainInts = MapSymbolsToInt(vocab, "<unk>", trainText)
    .checkPoint("ptbrnn/int_data_train.txt")
  val xvalInts = MapSymbolsToInt(vocab, "<unk>", xvalText)
    .checkPoint("ptbrnn/int_data_xval.txt")
  val evallInts = MapSymbolsToInt(vocab, "<unk>", evalText)
    .checkPoint("ptbrnn/int_data_eval.txt")

  val buckets = "4,8,16,32,64"
  val netSymbols = MakeRNNLMGraph(shape, buckets)
    .checkPoint("ptbrnn/symbol.json")


  val numIteration = 40
  val learnRate = Constant(p.BigDecimal("0.001"))
  val learnRateDecay = Constant(p.BigDecimal("0.8"))
  val batchSize = 32
  val gradientClip = Constant(p.BigDecimal("0.2"))
  val trainParam = RNNLMTrainingParameter(batchSize, "adam", learnRate, gradientClip)

  val train = RNNLMTraining(netSymbols, trainParam, trainInts, xvalInts,
    contextLength = 1, numIteration = numIteration,
    learnRateDecay = Some(learnRateDecay))

  var plotData = doc.LinePlotData()
  plotData = doc.AddYValuesToLinePlot(plotData, train.xvalResults.drop(10), "Model", "")
  val lossPlot = doc.DrawLinePlotPNG(plotData, doc.LinePlotFigStyle())
    .checkPoint("ptbrnn/training_plot.png")


}
