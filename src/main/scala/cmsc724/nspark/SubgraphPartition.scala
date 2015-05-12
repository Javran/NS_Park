package cmsc724.nspark

import java.io.File
import org.apache.spark._
import cmsc724.nspark._
import cmsc724.nspark.Type._
import org.apache.spark.rdd._
import SparkContext._
import Ordering.Implicits._

// implements subgraph partition
object SubgraphPartition {
  // since subgraphs are uniquely determined by query vertices
  // we also use NodeId of query vertices as the subgraph id
  type SubgraphId = NodeId

  // returns: RDD, number of partitions
  def calculatePacking(
    sp: SubgraphPredicate,
    sc: SparkContext,
    nodes: RDD[(NodeId, AttrSet)],
    edges: RDD[Edge]): (RDD[(SubgraphId, BinId)], Int) = {

    val shingleLimit: Int = 4
    val binWeightLimit: Long = 100L

    // extract nodes of interest using query vertices
    val nodesOfInterest: RDD[(NodeId, AttrSet)] = nodes.filter(sp.isQueryVertex)

    // extract 1-hop neighborhoods
    // TODO: more hops
    val edgesFromTo: RDD[(NodeId, Iterable[NodeId])] = edges.groupByKey
    // now we are using NodeId of query vertices as the subgraph Id
    val subgraphNeighborhoods1: RDD[(SubgraphId, (AttrSet, Iterable[NodeId]))] = nodesOfInterest join edgesFromTo

    // stage1: (node id, (set of attribute, weight, set of neighborhoods)
    val stage1: RDD[(SubgraphId, (AttrSet, Long, Iterable[NodeId]))] = {
      // (node,feature) join (node,[node]) => (node,(feature, [node]))
      def transformValue(
        v: (NodeId, (Set[String], Iterable[NodeId]))): (NodeId, (Set[String], Long, Iterable[NodeId])) = v match {
        case (nId, (fAttr, ns)) =>
          val w = fAttr.size + ns.size
          (nId, (fAttr, w, ns))
      }
      subgraphNeighborhoods1.map(transformValue)
    }

    // TODO: assume we only interest in 1-hop subgraph
    // then just adding the query node back should be enough to construct the subgraph
    // subgraphs: RDD of (SubgraphId, 
    val subgraphs: RDD[(SubgraphId, (AttrSet, Long, Set[NodeId]))] =
      stage1.map { case (k, (attr, w, s)) => (k, (attr, w, s.toSet + k)) }

    // TODO: take overlapping into account
    val subgraphWeightPairs: RDD[(NodeId, Long)] =
      subgraphs.map { case (k, (attr, w, s)) => (k, (w, s.toList.sorted.take(shingleLimit))) }
        .sortBy { case (k, (w, s)) => s }
        .map { case (k, (w, _)) => (k, w) }
    // have to scan data sequentially to determine packing
    // return a list of groups. --- this is the packing plan
    val partitionPlanStage1: List[List[(SubgraphId, Long)]] =
      Util.groupByWeight((x: (NodeId, Long)) =>
        x._2, binWeightLimit, subgraphWeightPairs.toLocalIterator.toList)
    val maxPartCount = partitionPlanStage1.size
    // tag packing plan with numbers, and flatten
    // queryId, bin
    val partitionPlanStage2: List[(SubgraphId, BinId)] =
      partitionPlanStage1
        .zipWithIndex
        .flatMap {
          case (grp, ind) =>
            grp.map {
              case (v, _) =>
                (v, ind)
            }
        }
    // go back to RDD
    val graphIdBinIdPairs: RDD[(SubgraphId, BinId)] =
      sc.parallelize(partitionPlanStage2)
    (graphIdBinIdPairs, maxPartCount)
  }
}