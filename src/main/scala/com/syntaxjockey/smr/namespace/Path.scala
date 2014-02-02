package com.syntaxjockey.smr.namespace

import scala.collection.IndexedSeqLike

final class Path private (val segments: Array[String], val length: Int) extends IndexedSeq[String] with IndexedSeqLike[String,Path] {
  import scala.collection.mutable

  override protected[this] def newBuilder: mutable.Builder[String, Path] = Path.newBuilder

  def apply(idx: Int): String = if (idx < 0 || length <= idx) throw new IndexOutOfBoundsException() else segments.apply(idx)

  override def toString(): String = "/" + segments.mkString("/")
}

object Path {
  import scala.collection.generic.CanBuildFrom
  import scala.collection.mutable

  def newBuilder: mutable.Builder[String,Path] = new mutable.ArrayBuffer[String] mapResult fromSeq

  def fromSeq(segments: Seq[String]): Path = {
    val length = segments.length
    if (length == 0) root else {
      val path = new Array[String](length)
      for (i <- 0 until length) {
        val segment = segments(i)
        // maintain invariants
        if (segment == "") throw new InvalidPathException("path contains empty segment")
        if (segment == "..") throw new InvalidPathException("path contains invalid segment '..'")
        if (segment == ".") throw new InvalidPathException("path contains invalid segment '.'")
        if (segment.contains("/")) throw new InvalidPathException("path contains invalid character '/' in segment")
        path(i) = segment
      }
      new Path(path, length)
    }
  }

  def apply(path: String): Path = {
    if (!path.startsWith("/")) throw new InvalidPathException("path is not absolute") else fromSeq(path.tail.split("/"))
  }

  def apply(path: Seq[String]): Path = fromSeq(path)

  implicit def canBuildFrom: CanBuildFrom[Path,String,Path] = new CanBuildFrom[Path,String,Path] {
    def apply(): mutable.Builder[String,Path] = newBuilder
    def apply(from: Path): mutable.Builder[String,Path] = newBuilder
  }

  val root: Path = new Path(new Array(0), 0)
}

object PathConversions {
  implicit def string2Path(string: String): Path = Path(string)
  implicit def seq2Path(seq: Seq[String]): Path = Path(seq)
}

