package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}
import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}
import ro.yota.weave.{doclib => doc}

/**
  * Subproject for training GMM acoustic models by maximum likelihood
  * 
  * @param res Resource object containing corpora and other artifacts
  * @param treeStats Statistics for initializing GMM parameters
  * @param nMix Number of mixture components per state
  * @param nDim Number of features per frame
  * @param nIte Number of iterations to be performed
  * @param realignIte
  *    If not None, realignment is performed only before the specified 
  *    iterations. Otherwise, realignment is performed before every iteration
  * @param updateOpts
  *    Configuration for the GMM update algorithm
  */
case class TrainGMMByMLE(
  res: AMTrainingResource,
  treeStats: Plan[spn.TreeStatsFile],
  nMix: Int,
  nDim: Int,
  nIte: Int,
  realignIte: Option[Set[Int]] = None,
  updateOpts: Plan[spn.UpdateGMMOpts] = spn.UpdateGMMOpts())
    extends Project {

  private[this] val realignIte_ = realignIte.getOrElse((0 until nIte).toSet);

  // Initialize
  val initGMM = spn.InitGMM(res.tree, treeStats, nMix, nDim)
  var curGMM: Plan[spn.GMMFile] = initGMM

  var curAlign = res.alignment(curGMM)


  // Training
  val allResults = for (ite <- (0 until nIte)) yield {
    if (realignIte_(ite)) {
      curAlign = res.alignment(curGMM)
    }
    val curGMMStatsList = (curAlign zip res.feature).map {
      case (align, feats) => spn.AccumulateGMMStats(align, feats, curGMM)
    }
    val curGMMStats = spn.MergeGMMStats(WeaveArray.fromSeq(curGMMStatsList))
    curGMM = spn.UpdateGMM(updateOpts, curGMM, curGMMStats).checkPoint(
      "mle_ite_%d".format(ite+1), level=CheckPointLevel.IntermediateResult)
    Tuple3(curGMM, curAlign, curGMMStats)
  }

  val allStats = allResults.map(_._3)
  val allGMMs = allResults.map(_._1)
  val allAligns = allResults.map(_._2)
}

