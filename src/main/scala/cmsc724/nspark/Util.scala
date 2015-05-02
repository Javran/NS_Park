package cmsc724.nspark

import java.io.File

object Util {
  // split a string (separated by spaces)
  // into a list of strings
  def splitWords(raw : String) : Seq[String] = {
    raw.split("\\s+").toList
  }

  // combine two files paths
  def combinePath(path1: String, path2: String): String = {
    val f1 = new File(path1)
    val f2 = new File(f1,path2)
    f2.getPath
  }

  def fileBaseName(f : File): String = {
    f.getName().takeWhile(_ != '.')
  }

  def addPair[A,B](p: (A,B), d: Map[A,Set[B]]): Map[A,Set[B]] = {
    p match {
      case (k,v) =>
        val oldV: Set[B] = d.applyOrElse(k, Function.const(Set()))
        d + (k -> oldV.+(v))
    }
  }
  def addBiPair[A](p: (A,A), d: Map[A,Set[A]]): Map[A,Set[A]] = {
    p match {
      case (a,b) => addPair((b,a),addPair((a,b),d))
    }
  }

  def safeToInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e:Exception => None
    }
  }
}