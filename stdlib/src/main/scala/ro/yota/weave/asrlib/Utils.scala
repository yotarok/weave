package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.{doclib => doc}


/**
  * Subproject for selecting best performing scorer
  * 
  * @param corpus Development set used for scorer evaluation
  * @param decodeOpts Decoder configuration
  * @param decoderNet Decoding FST (assuming static composition)
  * @param scorerList List of scorers to be evaluated
  * @param inputFlow Flow file for transforming input features
  * @param redefFST 
  *    If not None, hypotheses and reference words will be transduced
  *    with the given FST
  */
case class PickBestScorer(
  corpus: Corpus,
  decodeOpts: Plan[spn.DecodeOpts],
  decoderNet: Plan[fst.FSTFile],
  scorerList: Seq[Plan[spn.ScorerFile]],
  inputFlow: Plan[WeaveOption[p.File]],
  redefFST: Option[Plan[fst.FSTFile]]
)
    extends Project {
  val scores = (scorerList.map { scorer=>
    ParallelDecoder(
      corpus, decodeOpts, decoderNet, scorer, inputFlow, redefFST).score
  })

  val errorRates = scores.map(spn.ExtractWER(_))
  val errorRateArray = WeaveArray(errorRates:_*)
  val errorRatePlot = doc.DrawLinePlotPNG(
    doc.AddYValuesToLinePlot(doc.LinePlotData(),
      errorRateArray, "Error Rate", ""),
    doc.LinePlotFigStyle())
  val bestScorer = PickMinimizer(errorRateArray, WeaveArray(scorerList:_*))
}
