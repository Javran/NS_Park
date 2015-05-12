package cmsc724.nspark

import cmsc724.nspark.Type._

object TestUserConfiguration extends SubgraphPredicate
  with SubgraphComputation[Int]
  with Serializable {

  def isQueryVertex(n: Node): Boolean = {
    //attrSet.exists( p => p.contains("@amazon") )
    n._2.exists(p => p.contains("hometown;id") && p.contains("81"))

  }

  def isSubgraphVertex(n: Node): Boolean = true

  def computeOnSubgraph(
    edges: Iterable[Edge],
    vertices: Map[NodeId, AttrSet]): Int = {
    vertices.size
  }

  def combineResults(a: Int, b: Int): Int = {
    a + b
  }
  
  val zero: Int = 0
}