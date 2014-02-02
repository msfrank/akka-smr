package com.syntaxjockey.smr.namespace

import akka.util.ByteString
import org.joda.time.DateTime


/**
 * 
 */
case class Namespace(name: String, root: Node, version: Long, lastModified: DateTime) {
  import scala.annotation.tailrec

  @tailrec
  private def find(path: Path, root: Node): Option[Node] = {
    if (path == Path.root) Some(root) else {
      if (!root.children.contains(path.head)) None else find(path.tail, root.children(path.head))
    }
  }

  /**
   * Return Some(node) at the specified path, or None if the node doesn't exist.
   */
  def find(path: Path): Option[Node] = find(path, root)

  @tailrec
  private def exists(path: Path, root: Node): Boolean = {
    if (path == Path.root) true else {
      if (!root.children.contains(path.head)) false else exists(path.tail, root.children(path.head))
    }
  }

  /**
   * Return true if the specified path exists, otherwise false
   */
  def exists(path: Path): Boolean = exists(path, root)

  private def create(parent: Path, node: Node, name: String, data: ByteString, cversion: Long, ctime: DateTime): Node = {
    if (parent == Path.root) {
      if (node.children.contains(parent.head))
        throw new InvalidPathException("node already exists")
      val stat = Stat(data.length, 0, cversion, cversion, ctime, ctime, ctime)
      val child = Node(name, data, Map.empty, stat)
      Node(node.name, node.data, node.children + (name -> child), node.stat.copy(childrenVersion = cversion, modifiedChildren = ctime))
    } else {
      if (!node.children.contains(parent.head))
        throw new InvalidPathException("intermediate node doesn't exist")
      val child = create(parent.tail, node.children(parent.head), name, data, cversion, ctime)
      Node(node.name, node.data, node.children + (child.name -> child), node.stat.copy(childrenVersion = cversion, modifiedChildren = ctime))
    }
  }

  /**
   * Create a node at the specified path with the specified data.
   */
  def create(path: Path, data: ByteString): Namespace = {
    val lastModified = DateTime.now()
    val currentVersion = version + 1
    val currentRoot = create(path.init, root, path.last, data, currentVersion, lastModified)
    Namespace(name, currentRoot, currentVersion, lastModified)
  }
}

object Namespace {
  def apply(name: String) = {
    val timestamp = DateTime.now()
    val stat = Stat(
      dataLength = 0,
      numChildren = 0,
      dataVersion = 0,
      childrenVersion = 0,
      created = timestamp,
      modifiedData = timestamp,
      modifiedChildren = timestamp)
    val root = Node("", ByteString.empty, Map.empty, stat)
    new Namespace(name, root, stat.childrenVersion, timestamp)
  }
}

/**
 *
 */
case class Node(name: String, data: ByteString, children: Map[String,Node], stat: Stat)

/**
 * 
 */
case class Stat(dataLength: Int, numChildren: Int, dataVersion: Long, childrenVersion: Long, created: DateTime, modifiedData: DateTime, modifiedChildren: DateTime)

