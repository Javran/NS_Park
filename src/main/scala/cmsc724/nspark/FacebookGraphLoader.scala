package cmsc724.nspark

import org.apache.spark._
import org.apache.spark.rdd._
import cmsc724.nspark.Type._

object FacebookGraphLoader {
  def loadCircles(sc: SparkContext, fName: String): CircleDict = {
    val textFile = sc.textFile(fName)
    def toPair(xs: Seq[String]): (String, Set[NodeId]) = {
      // <key> <node id> ...
      (xs.head, xs.tail.map(_.toInt).toSet)
    }
    textFile.map(Util.splitWords _ andThen toPair _).collect().toMap
  }

  def loadEdges(sc: SparkContext, fName: String): EdgeDict = {
    val textFile = sc.textFile(fName)
    def toPair(xs: Seq[String]): Edge = {
      // <edge 1> <edge 2>
      (xs(0).toInt, xs(1).toInt)
    }
    val xs = textFile.map(Util.splitWords _ andThen toPair _).collect().toList
    // NOTE: edges are not "normalized" (i.e. ego edges are not yet added)
    xs.foldLeft(Map(): EdgeDict)((acc, p) => Util.addBiPair(p, acc))
  }

  def loadFeats(sc: SparkContext, fName: String): FeatDict = {
    val textFile = sc.textFile(fName)
    def toPair(xs: Seq[String]): (NodeId, Array[Feature]) = {
      (xs.head.toInt, xs.tail.map(_.toInt).toArray)
    }
    textFile.map(line => toPair(Util.splitWords(line))).collect().toMap
  }

  def loadEgoFeat(sc: SparkContext, fName: String): Array[Feature] = {
    val featLine = sc.textFile(fName).take(1)(0) // we only have one line
    Util.splitWords(featLine).map(_.toInt).toArray
  }

  def loadFeatNames(sc: SparkContext, fName: String): FeatNameDict = {
    val textFile = sc.textFile(fName)
    def toPair(xs: String): Array[String] = {
      val sep = xs.indexOf(" ")
      xs.splitAt(sep) match {
        case (k, v) => v.tail.split(";")
      }
    }
    val fSeq = textFile.map(line => toPair(line)).collect()
    fSeq.toArray
  }
}