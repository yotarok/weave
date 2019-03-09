package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.{doclib => doc}

/**
  * Subproject for running parallel decoding for split features
  * 
  * @param corpus Corpus containing features to be decoded
  * @param decodeOpts Configuration for decoders
  * @param decoderNet Decoding FST (assuming static composition)
  * @param scorer Scorer file (e.g. GMM or NN)
  * @param inputFlow Flow file for transforming input features
  * @param redefFST 
  *    If not None, hypotheses and reference words will be transduced
  *    with the given FST
  */
case class ParallelDecoder(
  corpus: Corpus,
  decodeOpts: Plan[spn.DecodeOpts],
  decoderNet: Plan[fst.FSTFile],
  scorer: Plan[spn.ScorerFile],
  inputFlow: Plan[WeaveOption[p.File]],
  redefFST: Option[Plan[fst.FSTFile]]) extends Project {

  val redefRefs = redefFST match {
    case None => corpus.wordGraph
    case Some(fst) =>
      for (wg <- corpus.wordGraph) yield {
        spn.ProjectCorpusFST(spn.ComposeCorpusAndFST(wg,
          fst, corpus.wordGraphTag, corpus.wordGraphTag, true),
          true, true, corpus.wordGraphTag, corpus.wordGraphTag)
      }
  }

  val rawHypos = for (feat <- corpus.feature) yield {
    spn.Decode(decodeOpts, feat, scorer, inputFlow, decoderNet)
  }
  val hypo = redefFST match {
    case None => rawHypos
    case Some(fst) =>
      for (hyp <- rawHypos) yield {
        spn.ComposeCorpusAndFST(hyp, fst,
          "decoded", "decoded", true)
      }
  }

  val scoreAndError = (for ((hyp, ref) <- (hypo zip redefRefs)) yield {
    val pair = spn.AlignHypothesis(hyp, "decoded",
      ref, corpus.wordGraphTag)
    (pair._1, pair._2)
  }).unzip

  val score = spn.MergeScore(WeaveArray.fromSeq(scoreAndError._1))
}
