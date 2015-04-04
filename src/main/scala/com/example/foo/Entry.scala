package com.example.foo

import org.apache.spark._
import org.apache.spark.rdd._

object Entry {
  type NodeId = Int
  // TODO: it's implicit that ego connects to every other nodes
  // so a direct query might not yield desired result.
  type Edge = (NodeId, NodeId)

  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
	val textFile = sc.textFile("data/facebook/0.edges")

	println(textFile.count())
  }

  def loadEdges(sc: SparkContext, fileName : String) : List[Edge] =
  {
    val tf = sc.textFile(fileName)
    val toPair : (Seq[String] => Edge) = {case (a::b::_) => (a.toInt,b.toInt) }
    val xs : RDD[Edge] = tf.map( line => toPair(line.split("\\s+").toSeq) )
    return xs.collect().toList
  }
}