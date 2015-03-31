package com.example.foo

import org.apache.spark._

object Entry {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
	val textFile = sc.textFile("data/facebook/0.edges")

	println(textFile.count())
  }

}