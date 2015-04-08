package cmsc724.nspark

import cmsc724.nspark.Type._

class FacebookFilePath(val basePath: String, val egoId: NodeId) {
  val egoIdStr: String = egoId.toString()
  private def withExtName(extName: String): String = {
    Util.combinePath(basePath, egoIdStr + "." + extName)
  }

  val circles: String    = withExtName("circles")
  val edges: String      = withExtName("edges")
  val egoFeat: String    = withExtName("egofeat")
  val feat: String       = withExtName("feat")
  val featNames: String  = withExtName("featnames")
}