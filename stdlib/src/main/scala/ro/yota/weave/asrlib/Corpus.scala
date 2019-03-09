package ro.yota.weave.asrlib

import ro.yota.weave._

import ro.yota.weave.{planned => p}

import ro.yota.weave.stdlib._
import ro.yota.weave.tools.{opengrm => grm}
import ro.yota.weave.tools.{openfst => fst}
import ro.yota.weave.tools.{spin => spn}


/**
  * Minimal corpora objects
  * 
  * The minimum ingredients for corpora in asrlib are features and words.
  * Corpora involving other kind of data like audio, or phonemes must be 
  * inherited from extended base classes
  */
trait Corpus extends Project {
  /** Files for split features */
  def feature: Seq[Plan[spn.CorpusFile]] 

  /** Tag name for features */
  def featureTag: String = "feature"

  /** Files for split data */
  def wordGraph: Seq[Plan[spn.CorpusFile]]

  /** Tag name for word graphs */
  def wordGraphTag: String = "wordgraph"

  /**
    * TO DO: This shouldn't be here. need to be refactored
    */
  def mergedTreeStats(alignments: Seq[Plan[spn.CorpusFile]],
    leftCtx: Int, rightCtx: Int)
      : Plan[spn.TreeStatsFile] = {
    val slist = for ((align, feat) <- alignments zip feature) yield {
      spn.AccumulateTreeStats(align, feat, leftCtx, rightCtx)
    }
    slist.toList match {
      case (elem :: Nil) => elem
      case _ => spn.MergeTreeStats(WeaveArray.fromSeq(slist))
    }
  }


  /** Merged feature corpus */
  def mergedFeature: Plan[spn.CorpusFile] = feature match {
    case (elem :: Nil) => elem
    case _ => spn.MergeCorpus(WeaveArray.fromSeq(feature))
  }
}


/**
  * Base class for corpora with raw audio signals
  */
trait AudioCorpus extends Corpus {
  def audio: Seq[Plan[spn.CorpusFile]]
  def featureExtractor: Plan[spn.CorpusFile]=>Plan[spn.CorpusFile]

  def feature = audio.map(featureExtractor)
}


/**
  * The base class for AMTrainingResources which is a bundle for everything 
  * required for acoustic model training
  */
trait AMTrainingResource extends Project {
  def corpus: Corpus
  def tree: Plan[spn.TreeFile]

  def phoneGraph: Seq[Plan[spn.CorpusFile]];
  def phoneGraphTag: String = "phonegraph"

  def stateGraph: Seq[Plan[spn.CorpusFile]] = {
    for (p <- phoneGraph) yield {
      spn.ComposeCorpusAndFST(p, stateContextFST,
        phoneGraphTag, stateGraphTag, false)
    }
  }
  def stateGraphTag: String = "stategraph"

  def disambPhonesStr: Plan[p.String] = ""

  def stateContextFST: Plan[fst.FSTFile] =
    fst.OptimizeFST(spn.ConvertTreeToHCFST(tree, this.disambPhonesStr))

  def alignOpts: Plan[spn.AlignOpts] =
    spn.AlignOpts()

  // utility functions
  def alignment(scorer: Plan[spn.ScorerFile]) =
    (stateGraph zip feature).map {
      case (states, feats) =>
        spn.Align(alignOpts, states, feats, scorer, WeaveNone)
    }

  def mergedAlignment(scorer: Plan[spn.ScorerFile]) = alignment(scorer) match {
    case (elem :: Nil) => elem
    case aligns => spn.MergeCorpus(WeaveArray.fromSeq(aligns))
  }

  def equalAlignment =
    (stateGraph zip feature).map {
      case (states, feats) =>
        spn.ComputeEqualAlignment(states, feats)
    }


  // shortcut
  def feature = corpus.feature
  def wordGraph = corpus.wordGraph
}

/**
  * AMTrainingResource containing lexicon
  */
trait AMTrainingResourceWithLexicon extends AMTrainingResource {
  def lexiconFST: Plan[fst.FSTFile]
  def phoneGraph: Seq[Plan[spn.CorpusFile]] =
    for (w <- corpus.wordGraph) yield {
      spn.ComposeCorpusAndFST(w, lexiconFST,
        corpus.wordGraphTag, phoneGraphTag, false)
    }
}

