package com.example.foo

import org.apache.spark._
import cmsc724.nspark.Util
import cmsc724.nspark.FacebookFilePath
import cmsc724.nspark.FacebookGraph
import org.apache.spark.rdd._

object Entry {

  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val paths = new FacebookFilePath("data/facebook",0)
    val g = new FacebookGraph(sc,paths)
    // g.circles.foreach(println)
    val nodes = g.pickNodes( (_,d) => d(Array("work","with","id","anonymized feature 205")) match {
      case None => false
      case Some(v) => v == 0
    }
    )
    nodes.foreach(println)
  }
}