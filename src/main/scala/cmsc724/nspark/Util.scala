package cmsc724.nspark

import java.io.File
import cmsc724.nspark.Type._

object Util {
  // scan edo nodes under a basePath
  def scanEgoNode(basePath: String): Set[NodeId] = {
     new File(basePath)
      .listFiles
      .flatMap(f => Util.safeToInt(Util.fileBaseName(f)))
      .toSet
  }
  
  // split a string (separated by spaces)
  // into a list of strings
  def splitWords(raw : String) : Seq[String] = {
    raw.split("\\s+").toList
  }

  // combine two files paths
  def combinePath(path1: String, path2: String): String = {
    val f1 = new File(path1)
    val f2 = new File(f1,path2)
    f2.getPath
  }

  def fileBaseName(f : File): String = {
    f.getName().takeWhile(_ != '.')
  }

  def addPair[A,B](p: (A,B), d: Map[A,Set[B]]): Map[A,Set[B]] = {
    p match {
      case (k,v) =>
        val oldV: Set[B] = d.applyOrElse(k, Function.const(Set()))
        d + (k -> oldV.+(v))
    }
  }
  def addBiPair[A](p: (A,A), d: Map[A,Set[A]]): Map[A,Set[A]] = {
    p match {
      case (a,b) => addPair((b,a),addPair((a,b),d))
    }
  }

  def safeToInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e:Exception => None
    }
  }

  def groupByWeight[T](select: T => Long, limit: Long, xs: List[T]) : List[List[T]] = {
    val ys: List[(T,Long)] = xs zip (  (xs.map(select).scanLeft(0L)(_ + _)).tail )
    def groupByWeightWithBase(base: Long, pairs: List[(T,Long)]) : List[List[(T,Long)]] = pairs match {
      case Nil => Nil
      case ((m,n) :: xs) if (n-base > limit) => List((m,n)) :: groupByWeightWithBase(n,xs)
      case _ =>
        val (left,right) = pairs.span( _._2 - base <= limit )
        left :: groupByWeightWithBase(left.last._2,right)
    }
    groupByWeightWithBase(0,ys).map( e => e.map( _._1 ) )
  }
}