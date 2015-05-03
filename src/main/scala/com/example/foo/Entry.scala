package com.example.foo

import java.io.File
import org.apache.spark._
import cmsc724.nspark.Util
import cmsc724.nspark.FacebookFilePath
import cmsc724.nspark.FacebookGraph
import cmsc724.nspark.Type._
import org.apache.spark.rdd._

object Entry {

  type FeatureAttr = (NodeId, Set[String])
  type NodePredicate = FeatureAttr => Boolean

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val paths = new FacebookFilePath("data/facebook", 0)

    // scan the directory to get all ego node names
    val allEgos: Set[NodeId] = new File(paths.basePath)
      .listFiles
      .flatMap(f => Util.safeToInt(Util.fileBaseName(f)))
      .toSet

    val allEgoFeaturesDict: Map[NodeId, RDD[FeatureAttr]] = allEgos
      .map(n => {
        val ps = new FacebookFilePath("data/facebook", n)
        val fAttr = loadFeatNameFile(ps.featNames, sc)
        val egoFeature = (ps.egoId, loadEgoFeature(ps.egoFeat, sc, fAttr))
        val allFeats = sc.parallelize(Seq(egoFeature)) ++ loadFeature(ps.feat, sc, fAttr)
        (n, allFeats)
      }).toMap

    val allEgoEdges: Map[NodeId, RDD[(NodeId,NodeId)]] = allEgos
      .map( n => {
        val ps = new FacebookFilePath("data/facebook", n)
        val edges: RDD[(NodeId,NodeId)] = loadEdges(ps.edges,sc)
        // get all nodes from feature list
        val nodes: RDD[NodeId] = allEgoFeaturesDict(n).map(_._1)
        // create edges from ego
        val edgesFromEgo: RDD[(NodeId,NodeId)] = nodes.withFilter(n2 => n2 != n).map( (n,_) )
        // note that for facebook, all edges are undirected,
        // therefore allUndirEdges are just half of the whole edge list
        val allUndirEdges: RDD[(NodeId,NodeId)] = edges ++ edgesFromEgo
        val allRevEdges: RDD[(NodeId,NodeId)] = allUndirEdges.map({case (a,b) => (b,a)})
        (n,allUndirEdges++allRevEdges)
      }).toMap

    val nodes: RDD[FeatureAttr] = allEgoFeaturesDict(0)
    val edges: RDD[(NodeId,NodeId)] = allEgoEdges(0)

    def testNodepred( fAttr : FeatureAttr ): Boolean = fAttr match {
      case (_,attrSet) => attrSet.exists( p => p.contains("hometown;id") && p.contains("81") )
    }
    val nodesOfInterest: RDD[FeatureAttr] = nodes.withFilter(testNodepred)
    nodesOfInterest.foreach(println)
  }

  def loadEdges(path: String, sc: SparkContext): RDD[(NodeId,NodeId)] = {
    sc.textFile(path).map(raw => {
      val splitted = Util.splitWords(raw).map(_.toInt)
      assert(splitted.length == 2)
      (splitted(0),splitted(1))
    })
  }

  def loadEgoFeature(path: String, sc: SparkContext, featNameArr: Array[String]): Set[String] = {
    val raw: RDD[String] = sc.textFile(path)
    assert(raw.count() == 1);
    val splitted = Util.splitWords(raw.first).map(_.toInt)
    assert(splitted.length == featNameArr.length)
    val attrs: Set[String] = (featNameArr zip splitted)
      .flatMap { case (a, i) => if (i == 1) Some(a) else None }
      .toSet
    attrs
  }

  def loadFeature(path: String, sc: SparkContext, featNameArr: Array[String]): RDD[FeatureAttr] = {
    sc.textFile(path).map(l => {
      val cols = Util.splitWords(l).map(_.toInt)
      assert(cols.tail.length == featNameArr.length)
      val attrs = (featNameArr zip cols.tail)
        .flatMap(p =>
          if (p._2 == 1) Some(p._1)
          else None)
        .toSet
      (cols.head, attrs)
    })
  }

  // load feature name files
  def loadFeatNameFile(path: String, sc: SparkContext): Array[String] = {
    def splitAtFirstSpace(xs: String): (Long, String) = xs.span(_ != ' ') match {
      case (l, c) => (l.toLong, c)
    }

    def splitAndVerify(raw: String, n: Long): String = splitAtFirstSpace(raw) match {
      case (l, c) => assert(n == l); c
    }
    sc.textFile(path)
      .zipWithIndex
      .map(d => d match { case (n, l) => splitAndVerify(n, l) })
      .toArray
  }
}