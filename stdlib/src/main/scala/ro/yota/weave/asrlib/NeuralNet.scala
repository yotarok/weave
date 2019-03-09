package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.{doclib => doc}

/**
  * Function object for preparing NN update options
  */
case object MakeUpdateOpt {
  def apply(
    learnRate: Plan[p.BigDecimal],
    momentum: Plan[p.BigDecimal],
    l2reg: Plan[p.BigDecimal])
      : Plan[spn.NNUpdateOpts] = {
    spn.SetL2Regularization(
      spn.SetMomentum(
        spn.SetLearningRate(spn.NNUpdateOpts(), "_", learnRate),
        "_", momentum),
      "_", l2reg)
  }

  def apply(): Plan[spn.NNUpdateOpts] = {
    val initLearnRate = BigDecimal("0.0001")
    val momentum = BigDecimal("0.9")
    val l2Regular = BigDecimal("0")
    this(initLearnRate, momentum, l2Regular)
  }
}

/**
  * Subproject for making simple DNN with fixed width and depth 
  *   without pretraining
  * 
  * @param nInput Number of input units
  * @param nHidUnit Number of hidden units per layer
  * @param nHidLayer Number of hidden layers
  * @param nOuptut Number of output classes
  * @param inputName Node name for input layer
  * @param hiddenType Node type for hidden layers
  * @param outputName Node name for output layer
  * @param randscale Scale parameter for random initializer
  * @param hiddenNameTemplate Format string for names of hidden activations
  * @param affineNameTemplate Format string for names of affine transforms
  */
case class MakeSimpleNN(
  nInput: Plan[p.Int], nHidUnit: Plan[p.Int],
  nHidLayer: Int, nOutput: Plan[p.Int],
  inputName: String = "input",
  hiddenType: String = "sigmoid",
  outputName: String = "output",
  randscale: BigDecimal = BigDecimal("1.0"),
  hiddenNameTemplate: String = "hid%d",
  affineNameTemplate: String = "aff%d") extends Project {
  val emptyNN = spn.CreateEmptyNN()
  var builtNN = spn.AddNodeToNN(
    emptyNN, "ident", inputName, WeaveArray(),
    WeaveArray("ndims" -> nInput.as[p.String])
  )
  var prevNodeName = inputName

  for (i <- 1 to nHidLayer) {
    val nIn = if (i == 1) { nInput } else { nHidUnit }
    val nOut = nHidUnit
    builtNN = spn.AddNodeToNN(builtNN, "affine",
      affineNameTemplate.format(i), WeaveArray(prevNodeName),
      WeaveArray(
        "ninputs" -> nIn.as[p.String],
        "noutputs" -> nOut.as[p.String], 
        "randscale" -> randscale.toString
      ))
    prevNodeName = affineNameTemplate.format(i)
    builtNN = spn.AddNodeToNN(builtNN, hiddenType,
      hiddenNameTemplate.format(i), WeaveArray(prevNodeName),
      WeaveArray(
        "ndims" -> nOut.as[p.String]
      ))
    prevNodeName = hiddenNameTemplate.format(i)
  }

  builtNN = spn.AddNodeToNN(builtNN, "affine",
    affineNameTemplate.format(nHidLayer+1), WeaveArray(prevNodeName),
    WeaveArray(
      "ninputs" -> nHidUnit.as[p.String],
      "noutputs" -> nOutput.as[p.String],
      "randscale" -> randscale.toString
    ))
  prevNodeName = affineNameTemplate.format(nHidLayer+1)
  builtNN = spn.AddNodeToNN(builtNN, "ident",
    outputName, WeaveArray(prevNodeName),
    WeaveArray(
      "ndims" -> nOutput.as[p.String]
    ))

}

/**
  * Subproject for making simple DNN with fixed width, depth, and dropout 
  *   without pretraining
  * 
  * @param nInput Number of input units
  * @param nHidUnit Number of hidden units per layer
  * @param nHidLayer Number of hidden layers
  * @param nOuptut Number of output classes
  * @param inputName Node name for input layer
  * @param hiddenType Node type for hidden layers
  * @param outputName Node name for output layer
  * @param randscale Scale parameter for random initializer
  * @param hiddenNameTemplate Format string for names of hidden activations
  * @param affineNameTemplate Format string for names of affine transforms
  */
case class MakeSimpleDropoutNN(
  nInput: Plan[p.Int], nHidUnit: Plan[p.Int],
  nHidLayer: Int, nOutput: Plan[p.Int],
  inputName: String = "input",
  hiddenType: String = "sigmoid",
  outputName: String = "output",
  dropoutFraction: BigDecimal = BigDecimal("0.5"),
  randscale: BigDecimal = BigDecimal("1.0"),
  hiddenNameTemplate: String = "hid%d",
  dropoutNameTemplate: String = "drop%d",
  affineNameTemplate: String = "aff%d") extends Project {

  val emptyNN = spn.CreateEmptyNN()
  var builtNN = spn.AddNodeToNN(
    emptyNN, "ident", inputName, WeaveArray(),
    WeaveArray("ndims" -> nInput.as[p.String])
  )
  var prevNodeName = inputName

  for (i <- 1 to nHidLayer) {
    val nIn = if (i == 1) { nInput } else { nHidUnit }
    val nOut = nHidUnit
    builtNN = spn.AddNodeToNN(builtNN, "affine",
      affineNameTemplate.format(i), WeaveArray(prevNodeName),
      WeaveArray(
        "ninputs" -> nIn.as[p.String],
        "noutputs" -> nOut.as[p.String], 
        "randscale" -> randscale.toString
      ))
    prevNodeName = affineNameTemplate.format(i)
    builtNN = spn.AddNodeToNN(builtNN, hiddenType,
      hiddenNameTemplate.format(i), WeaveArray(prevNodeName),
      WeaveArray(
        "ndims" -> nOut.as[p.String]
      ))
    prevNodeName = hiddenNameTemplate.format(i)
    builtNN = spn.AddNodeToNN(builtNN, "dropout",
      dropoutNameTemplate.format(i), WeaveArray(prevNodeName),
      WeaveArray(
        "ndims" -> nOut.as[p.String],
        "fraction" -> dropoutFraction.toString
      ))
    prevNodeName = dropoutNameTemplate.format(i)
  }

  builtNN = spn.AddNodeToNN(builtNN, "affine",
    affineNameTemplate.format(nHidLayer+1), WeaveArray(prevNodeName),
    WeaveArray(
      "ninputs" -> nHidUnit.as[p.String],
      "noutputs" -> nOutput.as[p.String],
      "randscale" -> randscale.toString
    ))
  prevNodeName = affineNameTemplate.format(nHidLayer+1)
  builtNN = spn.AddNodeToNN(builtNN, "ident",
    outputName, WeaveArray(prevNodeName),
    WeaveArray(
      "ndims" -> nOutput.as[p.String]
    ))

}


/**
  * Subproject for performing cross-entropy training with newbob learning rate
  *  scheduling
  */
case class RunXentTrainingWithNewBobScheduling(
  val trainStreams: Plan[WeaveArray[spn.NNStreamDef]],
  val xvalStreams: Plan[WeaveArray[spn.NNStreamDef]],
  val initNN: Plan[spn.NNFile],
  val nIte: Int,
  val initUpdateOpts: Plan[spn.NNUpdateOpts] = MakeUpdateOpt(),
  val batchOpts: Plan[spn.NNMiniBatchOpts] =
    spn.NNMiniBatchOpts().copy("batchsize" -> 1024, "seed" -> 0x5EED))
    extends Project {

  var curNN = initNN
  val initFER = spn.ExtractNNFrameErrorRate(
    spn.EvaluateNN(curNN, xvalStreams), "output")
  var prevFER: Plan[p.BigDecimal] = initFER
  var curUpdateOpts = initUpdateOpts
  val results = for (ite <- 1 to nIte) yield {
    planWith(trace = s"Iteration ${ite}") {
      curNN = spn.TrainNNByShuffledSGD(curNN,
        batchOpts.copy("seed" -> (0x5EED5EED + ite)),
        trainStreams, curUpdateOpts)
        .checkPoint(
        "xent_ite%d".format(ite), level=CheckPointLevel.IntermediateResult)

      val curFER = spn.ExtractNNFrameErrorRate(
        spn.EvaluateNN(curNN, xvalStreams), "output")
        .checkPoint("xent_ite%d.error".format(ite),
          level=CheckPointLevel.IntermediateResult)

      curUpdateOpts = spn.UpdateLearningRateWithNewBob(curUpdateOpts,
        prevFER, curFER, true, BigDecimal("0.5"), BigDecimal("0.99"))

      prevFER = curFER
      (curNN, curFER, curUpdateOpts)
    }
  }

  val allNNs = results.map(_._1)
  val allFERs = results.map(_._2)
  val allUpdateOpts = results.map(_._3)
  val allLRs = allUpdateOpts.map(spn.GetMaximumLearningRate(_))

  val trainedNN = allNNs.last
}


/**
  * Subproject for making neural nets deeper by inserting random hidden layer
  * 
  * A typical use case for this subproject is discriminative pretraining
  */
case class MakeDeeperNN(
  currentNN: Plan[spn.NNFile],
  nHidUnit: Plan[p.Int], currentNHidLayer: Int, nOutput: Plan[p.Int],
  inputName: String = "input",
  hiddenType: String = "sigmoid",
  outputName: String = "output",
  randscale: BigDecimal = BigDecimal("1.0"),
  hiddenNameTemplate: String = "hid%d",
  affineNameTemplate: String = "aff%d") extends Project {

  // delete output and last affine transform
  val featNN = spn.RemoveNodeFromNN(currentNN,
    WeaveArray(outputName, affineNameTemplate.format(currentNHidLayer + 1)))

  val addAff1 = spn.AddNodeToNN(featNN, "affine",
    affineNameTemplate.format(currentNHidLayer+1),
    WeaveArray(hiddenNameTemplate.format(currentNHidLayer)),
    WeaveArray(
      "ninputs" -> nHidUnit.as[p.String],
      "noutputs" -> nHidUnit.as[p.String],
      "randscale" -> randscale.toString
    ))

  val addHid = spn.AddNodeToNN(addAff1, hiddenType,
    hiddenNameTemplate.format(currentNHidLayer+1),
    WeaveArray(affineNameTemplate.format(currentNHidLayer+1)),
    WeaveArray(
      "ndims" -> nHidUnit.as[p.String]
    ))

  val addAff2 = spn.AddNodeToNN(addHid, "affine",
    affineNameTemplate.format(currentNHidLayer+2),
    WeaveArray(hiddenNameTemplate.format(currentNHidLayer+1)),
    WeaveArray(
      "ninputs" -> nHidUnit.as[p.String],
      "noutputs" -> nOutput.as[p.String],
      "randscale" -> randscale.toString
    ))
  val builtNN = spn.AddNodeToNN(addAff2, "ident",
    outputName, WeaveArray(affineNameTemplate.format(currentNHidLayer+2)),
    WeaveArray(
      "ndims" -> nOutput.as[p.String]
    ))
}

case class ReinitializeNNOutput(
  currentNN: Plan[spn.NNFile],
  nHidUnit: Plan[p.Int], currentNHidLayer: Int, nOutput: Plan[p.Int],
  outputName: String = "output",
  randscale: BigDecimal = BigDecimal("1.0"),
  hiddenNameTemplate: String = "hid%d",
  affineNameTemplate: String = "aff%d") extends Project {

  // delete output and last affine transform
  val featNN = spn.RemoveNodeFromNN(currentNN,
    WeaveArray(outputName, affineNameTemplate.format(currentNHidLayer + 1)))

  val addAff = spn.AddNodeToNN(featNN, "affine",
    affineNameTemplate.format(currentNHidLayer + 1),
    WeaveArray(hiddenNameTemplate.format(currentNHidLayer)),
    WeaveArray(
      "ninputs" -> nHidUnit.as[p.String],
      "noutputs" -> nOutput.as[p.String],
      "randscale" -> randscale.toString
    ))

  val builtNN = spn.AddNodeToNN(addAff, "ident",
    outputName, WeaveArray(affineNameTemplate.format(currentNHidLayer+1)),
    WeaveArray(
      "ndims" -> nOutput.as[p.String]
    ))

}


/**
  * Subproject for constructing neural nets with discriminative pretraining
  */
case class MakeNNWithDiscrPreTraining(
  val trainStreams: Plan[WeaveArray[spn.NNStreamDef]],
  val nInput: Plan[p.Int],
  val nHidUnit: Plan[p.Int],
  val nHidLayer: Int,
  val nOutput: Plan[p.Int],
  val hiddenType: String = "sigmoid",
  val updateOpts: Plan[spn.NNUpdateOpts] = MakeUpdateOpt(),
  val batchOpts: Plan[spn.NNMiniBatchOpts] =
    spn.NNMiniBatchOpts().copy("batchsize" -> 128, "seed" -> 0x5EED),
  val randscale: BigDecimal = BigDecimal("1.0")) extends Project {

  val initNN = MakeSimpleNN(nInput, nHidUnit, 1, nOutput, hiddenType=hiddenType,
    randscale=randscale).builtNN

  val initPreTrainedNN = spn.TrainNNByShuffledSGD(initNN,
    batchOpts, trainStreams, updateOpts).checkPoint(
    "pretrain_1", level=CheckPointLevel.IntermediateResult)

  var curPreTrainedNN = initPreTrainedNN
  val allPreTrainedNNs = Seq(initPreTrainedNN) ++ (
    for (nHid <- 1 to (nHidLayer - 1)) yield {
      curPreTrainedNN = MakeDeeperNN(curPreTrainedNN,
        nHidUnit, nHid, nOutput, hiddenType=hiddenType, randscale=randscale).builtNN
      
      curPreTrainedNN = spn.TrainNNByShuffledSGD(curPreTrainedNN,
        batchOpts.copy("seed" -> (0x5EED + nHid)),
        // ^ seed is overwritten ignoring the given value would it be a problem?
        trainStreams, updateOpts).checkPoint(
        "pretrain_%d".format(nHid + 1),
          level = CheckPointLevel.IntermediateResult)
      curPreTrainedNN
    })
  val preTrainedNN = allPreTrainedNNs.last
}

