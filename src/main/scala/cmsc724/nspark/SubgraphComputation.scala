package cmsc724.nspark

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

trait SubgraphComputation[T] {
  def computeOnSubgraph(edges: Iterable[Edge], vertices: Map[NodeId,AttrSet]): T
  def combineResults(a: T, b: T): T
  val zero: T
}