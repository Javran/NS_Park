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
        val edges = loadEdges(ps.edges,sc)
        (n,edges)
      }).toMap
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