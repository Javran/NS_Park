package cmsc724.nspark

object Type {
  type NodeId = Int
  type Edge = (NodeId, NodeId)
  type Attr = String
  type AttrSet = Set[Attr]
  type Node = (NodeId, AttrSet)
  type BinId = Int
}