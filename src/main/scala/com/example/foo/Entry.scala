package com.example.foo

import org.apache.spark._
import org.apache.spark.rdd._

object Entry {
  type NodeId = Int
  // TODO: it's implicit that ego connects to every other nodes
  // so a direct query might not yield desired result.
  type Edge = (NodeId, NodeId)
  type Feature = Int
  type FeatTable = Map[NodeId,Array[Feature]]
  type CircleTable = Map[String,Set[NodeId]]
  type FeatNames = Map[Int,Array[String] /* feat desc */ ] // indexed by feature id

  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
	loadEdges(sc,"data/facebook/0.edges").foreach(println)
	loadCircles(sc,"data/facebook/0.circles").foreach(println)
	println(loadEgoFeat(sc,"data/facebook/0.egofeat").length)
	loadFeats(sc,"data/facebook/0.feat").foreach(println)
	loadFeatNames(sc,"data/facebook/0.featnames").foreach( line => {
	  println(line._1 + ": " + line._2.mkString(">>"))
	})
  }

  def loadEdges(sc : SparkContext, fName : String) : List[Edge] = {
    val textFile = sc.textFile(fName)
    def toPair (xs :Seq[String]) : Edge = {
      (xs(0).toInt,xs(1).toInt)
    }
	val xs = textFile.map( line => toPair(splitWords(line)) )
    xs.collect().toList
  }

  def loadCircles(sc: SparkContext, fName : String) : CircleTable = {
    val textFile = sc.textFile(fName)
    def toPair (xs : Seq[String]) : (String, Set[NodeId]) = {
      (xs.head,xs.tail.map( _.toInt ).toSet)
    }
    textFile.map( line => toPair(splitWords(line)) ).collect().toMap
  }

  def loadEgoFeat(sc: SparkContext, fName : String) : Array[Feature]= {
    val featLine = sc.textFile(fName).take(1)(0) // we only have one line
    splitWords(featLine).map(_.toInt).toArray
  }

  def loadFeats(sc: SparkContext, fName: String) : FeatTable = {
    val textFile = sc.textFile(fName)
    def toPair (xs : Seq[String]) : (NodeId, Array[Feature]) = {
      (xs.head.toInt,xs.tail.map( _.toInt ).toArray)
    }
    textFile.map( line => toPair(splitWords(line)) ).collect().toMap
  }

  def loadFeatNames(sc: SparkContext, fName: String) : FeatNames = {
    val textFile = sc.textFile(fName)
    def toPair (xs : String) : (NodeId, Array[String]) = {
      val sep = xs.indexOf(" ")
      xs.splitAt(sep) match {
        case (k,v) => (k.toInt,v.tail.split(";"))
      }
    }
    textFile.map( line => toPair(line)).collect().toMap
  }


  def splitWords(raw : String) : List[String] = {
    raw.split("\\s+").toList
  }

}