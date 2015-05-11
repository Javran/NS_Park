package cmsc724.nspark
import cmsc724.nspark.Type._
import org.apache.spark.rdd._

import java.io.File
import org.apache.spark._
import cmsc724.nspark.Type._
import org.apache.spark.rdd._
import SparkContext._

// information related to the data set
// including file paths and if the edges are bidirectional
class DataSetInfo(val basePath: String, val egoNodeId: NodeId, val bidirectional: Boolean) extends Serializable {
  val egoNodeIdStr: String = egoNodeId.toString()
  private def withExtName(extName: String): String = {
    Util.combinePath(basePath, egoNodeIdStr + "." + extName)
  }

  val circlesPath: String = withExtName("circles")
  val edgesPath: String = withExtName("edges")
  val egoFeaturePath: String = withExtName("egofeat")
  val featuresPath: String = withExtName("feat")
  val featureNamesPath: String = withExtName("featnames")

  def loadGraph(sc: SparkContext): (RDD[(NodeId, AttrSet)], RDD[Edge]) = {
    val nodes = loadNodes(sc)
    val edges = loadEdges(sc, nodes)
    (nodes, edges)
  }

  def loadNodes(sc: SparkContext): RDD[(NodeId, AttrSet)] = {
    val fAttr = loadFeatNameFile(featureNamesPath, sc)
    val egoFeature = (egoNodeId, loadEgoFeature(egoFeaturePath, sc, fAttr))
    val allFeats = sc.parallelize(Seq(egoFeature)) ++ loadFeature(featuresPath, sc, fAttr)
    allFeats
  }

  private def loadEdges(sc: SparkContext, nodes: RDD[(NodeId, AttrSet)]): RDD[Edge] = {
    val edges: RDD[(NodeId, NodeId)] = loadEdges(edgesPath, sc)
    // get all nodes from feature list ... but here we are only interested in node Ids
    val nodeIds: RDD[NodeId] = nodes.map(_._1)
    // create edges from ego
    val edgesFromEgo: RDD[Edge] =
      nodeIds.filter(_ != egoNodeId).map((egoNodeId, _))
    // collect all directed edges here
    val allDirectedEdges: RDD[(NodeId, NodeId)] = edges ++ edgesFromEgo
    if (bidirectional) {
      val allRevEdges: RDD[(NodeId, NodeId)] =
        allDirectedEdges.map({ case (a, b) => (b, a) })
      allDirectedEdges ++ allRevEdges
    } else allDirectedEdges
  }

  private def loadEdges(path: String, sc: SparkContext): RDD[(NodeId, NodeId)] = {
    sc.textFile(path).map(raw => {
      val splitted = Util.splitWords(raw).map(_.toInt)
      assert(splitted.length == 2)
      (splitted(0), splitted(1))
    })
  }

  // load feature name files
  private def loadFeatNameFile(path: String, sc: SparkContext): Array[String] = {
    sc.textFile(path)
      .zipWithIndex
      .map { case (n, l) => splitAndVerify(n, l) }
      .toArray
  }

  private def loadFeature(path: String, sc: SparkContext, featNameArr: Array[String]): RDD[(NodeId, AttrSet)] = {
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

  private def splitAtFirstSpace(xs: String): (Long, String) = xs.span(_ != ' ') match {
    case (l, c) => (l.toLong, c)
  }

  private def splitAndVerify(raw: String, n: Long): String = splitAtFirstSpace(raw) match {
    case (l, c) => assert(n == l); c
  }

  private def loadEgoFeature(path: String, sc: SparkContext, featNameArr: Array[String]): Set[String] = {
    val raw: RDD[String] = sc.textFile(path)
    assert(raw.count() == 1);
    val splitted = Util.splitWords(raw.first).map(_.toInt)
    assert(splitted.length == featNameArr.length)
    val attrs: Set[String] = (featNameArr zip splitted)
      .flatMap { case (a, i) => if (i == 1) Some(a) else None }
      .toSet
    attrs
  }
}