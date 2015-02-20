package com.syntaxjockey.smr.world

import java.net.URI

import com.syntaxjockey.smr.command.InvalidPathException

import scala.collection.IndexedSeqLike

final class Path private (val segments: Array[String], val length: Int) extends IndexedSeq[String] with IndexedSeqLike[String,Path] with Serializable {
  import scala.collection.mutable

  override protected[this] def newBuilder: mutable.Builder[String, Path] = Path.newBuilder

  def apply(idx: Int): String = if (idx < 0 || length <= idx) throw new IndexOutOfBoundsException() else segments.apply(idx)

  override def toString(): String = "/" + segments.mkString("/")

  def isRoot = isEmpty

  def notRoot = nonEmpty

  def dirpath: Path = if (length <= 1) Path.root else init

  def entryname: String = last
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
    if (path == "/") fromSeq(Seq.empty) else {
      if (!path.startsWith("/"))
        throw new InvalidPathException("path is not absolute")
      if (path.endsWith("/"))
        throw new InvalidPathException("path must not end with a path separator")
      fromSeq(path.tail.split("/"))
    }
  }

  def apply(path: Seq[String]): Path = fromSeq(path)

  implicit def canBuildFrom: CanBuildFrom[Path,String,Path] = new CanBuildFrom[Path,String,Path] {
    def apply(): mutable.Builder[String,Path] = newBuilder
    def apply(from: Path): mutable.Builder[String,Path] = newBuilder
  }

  val root: Path = new Path(new Array(0), 0)
}

case class NamespacePath(namespace: String, path: Path)

object PathConversions {
  import scala.language.implicitConversions
  implicit def string2Path(string: String): Path = Path(string)
  implicit def seq2Path(seq: Seq[String]): Path = Path(seq)
  implicit def uri2NamespacePath(uri: URI): NamespacePath = NamespacePath(uri.getHost, Path(uri.getPath))
}

