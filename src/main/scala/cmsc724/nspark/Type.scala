package cmsc724.nspark

object Type {
  type NodeId = Int
  type Edge = (NodeId, NodeId)
  type CircleDict = Map[String,Set[NodeId]]
  type EdgeDict = Map[NodeId,Set[NodeId]]
  type Feature = Int
  type FeatDict = Map[NodeId,Array[Feature]]
  type FeatNameDict = Array[Array[String]]
}