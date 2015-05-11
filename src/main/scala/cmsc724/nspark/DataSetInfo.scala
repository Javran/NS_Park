package cmsc724.nspark
import cmsc724.nspark.Type._

// information related to the data set
// including file paths and if the edges are bidirectional
class DataSetInfo (val basePath: String, val egoNodeId: NodeId, val bidirectional: Boolean){
  val egoNodeIdStr: String = egoNodeId.toString()
  private def withExtName(extName: String): String = {
    Util.combinePath(basePath, egoNodeIdStr + "." + extName)
  }

  val circlesPath    : String = withExtName("circles")
  val edgesPath      : String = withExtName("edges")
  val egoFeaturePath : String = withExtName("egofeat")
  val featuresPath   : String = withExtName("feat")
  val featNamesPath  : String = withExtName("featnames")
}