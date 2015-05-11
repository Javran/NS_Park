package com.example.foo

import java.io.File
import org.apache.spark._
import cmsc724.nspark.Util
import cmsc724.nspark.FacebookFilePath
import cmsc724.nspark.DataSetInfo
import cmsc724.nspark.FacebookGraph
import cmsc724.nspark.Type._
import org.apache.spark.rdd._
import SparkContext._

object Entry {

  type FeatureAttr = (NodeId, Set[String])
  type NodePredicate = FeatureAttr => Boolean

  class SimplePartitioner(partitionCount: Int) extends Partitioner {
    def getPartition(key: Any):Int = {
      val k = key.asInstanceOf[Int]
      return k;
    }
    def numPartitions = partitionCount
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val dataSetInfo = new DataSetInfo("data/facebook",0,true)
    // val dataSetInfo = new DataSetInfo("data/twitter",14807093,false)

    val (nodes,edges)= dataSetInfo.loadGraph(sc)

    def testNodepred( fAttr : FeatureAttr ): Boolean = fAttr match {
      case (_,attrSet) =>
        //attrSet.exists( p => p.contains("@amazon") )
        attrSet.exists( p => p.contains("hometown;id") && p.contains("81") )
    }
    val nodesOfInterest: RDD[(NodeId,Set[String])] = nodes.withFilter(testNodepred)

    // (node,feature) join (node,[node]) => (node,(feature, [node]))
    def transformValue(v: (NodeId, (Set[String], Iterable[NodeId]))): (NodeId, (Set[String], Long, Iterable[NodeId])) = v match {
      case (nId,(fAttr,ns)) =>
        val w = fAttr.size + ns.size
        (nId,(fAttr,w,ns))
    }
    val edges1: RDD[(NodeId, Iterable[NodeId])] = edges.groupByKey
    val stage1_join: RDD[(NodeId, (Set[String], Iterable[NodeId]))] = nodesOfInterest join edges1
    val stage1: RDD[(NodeId, (Set[String], Long, Iterable[NodeId]))] = stage1_join.map(transformValue)
    // stage1: (node id, (set of attribute, weight, set of neighborhoods)
    // assume we only interest in 1-hop subgraph
    // then just adding the query node back should be enough to construct the subgraph
    val subgraphs: RDD[(NodeId, (Set[String], Long, Set[NodeId]))] =
      stage1.map { case (k, (attr, w, s)) => (k, (attr, w, s.toSet + k)) }
    val shingleLimit: Int = 4
    val subgraphWeightPairs: RDD[(NodeId, Long)] =
      subgraphs.map { case (k, (attr, w, s)) => (k, (w, s.toList.sorted.take(shingleLimit))) }
        .sortBy { case (k, (w, s)) => s.mkString("") }
        .map { case (k, (w, _)) => (k, w) }
    val partitionPlanStage1: List[ List[(NodeId,Long)] ] =
      Util.groupByWeight( (x:(NodeId,Long)) => x._2 , 100L, subgraphWeightPairs.toLocalIterator.toList)
    val maxPartCount = partitionPlanStage1.size
    // queryId, bin
    val partitionPlanStage2: List[(NodeId, Int)] =
      partitionPlanStage1.zipWithIndex.flatMap { case (grp, ind) => grp.map { case (v, _) => (v, ind) } }

    val graphIdBinIdPairs: RDD[(NodeId, Int)] = sc.parallelize(partitionPlanStage2)

    // get query graph
    val binNodePairs: RDD[(Int, NodeId)] =
      graphIdBinIdPairs
        .join(subgraphs)
        .flatMap { case (nId, (binId, (_, _, sbNodes))) => sbNodes.map((binId, _)) }
    // --------- bin packing is done
    val parts: RDD[(Int, Iterable[(Int, NodeId)])] =
      binNodePairs.groupBy(((p: (Int, NodeId)) => p._1), new SimplePartitioner(maxPartCount))

    val parts1: RDD[NodeId] = parts.flatMap { case (_, it) => it.map(_._2) }
//    val parts1: RDD[NodeId] = parts.mapPartitions( (it:Iterator[(Int,Iterable[(Int,NodeId)])])  => {
//      it.flatMap {case (_,itb) => itb.map{case (i,n) => n} }
//    }, true)

    // vertices sent to different workers
    val vsWithAttrs: RDD[(NodeId, Set[String])] = parts1
      .map((_, ()))
      .join(nodes).map { case (nId, (_, attr)) => (nId, attr) }

    vsWithAttrs.foreach(println)
//    // * re-apply Pqv, extracting edges
//    val nodesOfInterest2: RDD[(NodeId,Set[String])] = vsWithAttrs.withFilter(testNodepred)
//    // * get 1-hop neighborhoods
//    val subgraphParts: RDD[(NodeId, (Set[String], Iterable[NodeId]))] =
//      nodesOfInterest2
//        .join(edges1)
//
//    //val partsQueryVertices: RDD[]
//    // * collect vertices and edges, and send them to the user specified computation
//    val edgesOfInterest: RDD[(NodeId,NodeId)] = ???
  }

  def loadEdges(path: String, sc: SparkContext): RDD[(NodeId, NodeId)] = {
    sc.textFile(path).map(raw => {
      val splitted = Util.splitWords(raw).map(_.toInt)
      assert(splitted.length == 2)
      (splitted(0),splitted(1))
    })
  }

}