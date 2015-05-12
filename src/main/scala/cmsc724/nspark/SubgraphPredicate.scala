package cmsc724.nspark

import java.io.File
import cmsc724.nspark.Type._

trait SubgraphPredicate {
   def isQueryVertex (n: Node): Boolean
   // not used.
   // val numHop: Int
   def isSubgraphVertex (n: Node): Boolean
}