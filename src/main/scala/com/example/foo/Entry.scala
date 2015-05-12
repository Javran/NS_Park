package com.example.foo

import java.io.File
import org.apache.spark._
import cmsc724.nspark._
import cmsc724.nspark.Util
import cmsc724.nspark.DataSetInfo
import cmsc724.nspark.Type._
import cmsc724.nspark.SubgraphPredicate
import org.apache.spark.rdd._
import SparkContext._
import Ordering.Implicits._

object Entry {

  class SimplePartitioner(partitionCount: Int) extends Partitioner {
    def getPartition(key: Any):Int = {
      val k = key.asInstanceOf[(BinId,NodeId)]
      return k._1;
    }
    def numPartitions = partitionCount
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val dataSetInfo = new DataSetInfo("data/facebook", 0, true)
    // val dataSetInfo = new DataSetInfo("data/twitter",14807093,false)
    val (nodes, edges) = dataSetInfo.loadGraph(sc)

    val userConf = TestUserConfiguration
    val (graphIdBinIdPairs, maxPartCount) = SubgraphPartition.calculatePacking(userConf, sc, nodes, edges)

    // * get all subgraph vertices
    val edgesFromTo: RDD[(NodeId, Iterable[NodeId])] = edges.groupByKey
    val subgraphIdBinIdNeighborhoods: RDD[(NodeId, (BinId, Set[NodeId]))] =
      graphIdBinIdPairs
        .join(edgesFromTo)
        .map {
          case (sgId, (binId, itr)) =>
            (sgId, (binId, itr.toSet + sgId))
        } // TODO: 1-hop

    val subgraphIdBinIdNeighborhoods2: RDD[((BinId,NodeId), Set[NodeId])] =
      subgraphIdBinIdNeighborhoods.map { case (sgId,(binId,s)) => ((binId,sgId),s) }
    // * get related edges
    // NodeId, (BinId, NodeId)  // duplicate node id, to join with edges
    val subgraphDup: RDD[(NodeId,(BinId,NodeId))] =
      graphIdBinIdPairs.map { case (sgId,binId) => (sgId,(binId,sgId)) }
    val subgraphBinWithEdges: RDD[(NodeId,((BinId,NodeId),NodeId))] =
      subgraphDup join edges
    // rearrange tuple
    val subgraphBinWithEdges2: RDD[((BinId,NodeId),Iterable[Edge])] =
      subgraphBinWithEdges.map {case (vL,((binId,sgId),vR)) => ((binId,sgId),(vL,vR)) }
       .groupByKey
       
    val subgraphDispatch: RDD[ ((BinId,NodeId),(Set[NodeId],Iterable[Edge])) ] =
      subgraphIdBinIdNeighborhoods2.join(subgraphBinWithEdges2, new SimplePartitioner(maxPartCount))

    val result: Int = subgraphDispatch.map {
      case ((binId, sgId), (vs, es)) =>
        userConf.computeOnSubgraph(es, vs.foldLeft(Map[NodeId, AttrSet]()) { (m, s) => m + (s -> Set.empty) })
    }.fold(userConf.zero)(userConf.combineResults)
    // * get all related edges
    println(result)
  }

}