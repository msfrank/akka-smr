package com.syntaxjockey.smr.namespace

import akka.util.ByteString
import org.joda.time.DateTime
import scala.util.{Failure, Success, Try}


/**
 * 
 */
case class Namespace(name: String, version: Long, lastModified: DateTime, root: Node) {
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

  private def create(parent: Path, node: Node, name: String, data: ByteString, cversion: Long, ctime: DateTime): Try[Node] = {
    if (parent == Path.root) {
      if (node.children.contains(name)) Failure(new InvalidPathException("node already exists")) else {
        val child = Node(name, data, Stat(data.length, 0, cversion, cversion, ctime, ctime, ctime), Map.empty)
        Success(Node(node.name, node.data,
          node.stat.copy(childrenVersion = cversion, modifiedChildren = ctime),
          node.children + (name -> child)))
      }
    } else {
      if (!node.children.contains(parent.head)) Failure(new InvalidPathException("intermediate node doesn't exist")) else {
        create(parent.tail, node.children(parent.head), name, data, cversion, ctime) match {
          case Success(child) =>
            Success(Node(node.name, node.data,
              node.stat.copy(childrenVersion = cversion, modifiedChildren = ctime),
              node.children + (child.name -> child)))
          case Failure(ex) =>
            Failure(ex)
        }
      }
    }
  }

  /**
   * Create a node at the specified path with the specified data.
   */
  def create(path: Path, data: ByteString, ctime: DateTime): Try[Namespace] = {
    if (path == Path.root) Failure(new RootModification()) else {
      val cversion = version + 1
      create(path.init, root, path.last, data, cversion, ctime) match {
        case Success(croot) =>
          Success(Namespace(name, cversion, ctime, croot))
        case Failure(ex) =>
          Failure(ex)
      }
    }
  }

  private def delete(parent: Path, node: Node, name: String, dversion: Option[Long], mversion: Long, mtime: DateTime): Try[Node] = {
    if (parent == Path.root) {
      node.children.get(name) match {
        case None =>
          Failure(new InvalidPathException("node doesn't exist"))
        case Some(child) if dversion.isDefined && dversion.get != child.stat.dataVersion =>
          Failure(new VersionMismatch(dversion.get, child.stat.dataVersion))
        case Some(child) =>
          Success(Node(node.name, node.data,
            node.stat.copy(childrenVersion = mversion, modifiedChildren = mtime),
            node.children - name))
      }
    } else {
      if (!node.children.contains(parent.head)) Failure(new InvalidPathException("intermediate node doesn't exist")) else {
        delete(parent.tail, node.children(parent.head), name, dversion, mversion, mtime) match {
          case Success(child) =>
            Success(Node(node.name, node.data,
              node.stat.copy(childrenVersion = mversion, modifiedChildren = mtime),
              node.children + (child.name -> child)))
          case Failure(ex) =>
            Failure(ex)
        }
      }
    }
  }

  /**
   * Delete the node at the specified path, and all of its children.
   */
  def delete(path: Path, dversion: Option[Long], mtime: DateTime): Try[Namespace] = {
    if (path == Path.root) Failure(new RootModification()) else {
      val mversion = version + 1
      delete(path.init, root, path.last, dversion, mversion, mtime) match {
        case Success(mroot) =>
          Success(Namespace(name, mversion, mtime, mroot))
        case Failure(ex) =>
          Failure(ex)
      }
    }
  }

  /**
   * Flush the namespace, deleting all children of root and creating a new
   * root node with no data.
   */
  def flush(mtime: DateTime): Try[Namespace] = {
    val stat = Stat(0, 0, 0, 0, mtime, mtime, mtime)
    val root = Node("", ByteString.empty, stat, Map.empty)
    Success(new Namespace(name, version + 1, mtime, root))
  }
}

object Namespace {
  def apply(name: String) = {
    val timestamp = DateTime.now()
    val stat = Stat(0, 0, 0, 0, timestamp, timestamp, timestamp)
    val root = Node("", ByteString.empty, stat, Map.empty)
    new Namespace(name, stat.childrenVersion, timestamp, root)
  }
}

/**
 *
 */
case class Node(name: String, data: ByteString, stat: Stat, children: Map[String,Node])

/**
 * 
 */
case class Stat(dataLength: Int, numChildren: Int, dataVersion: Long, childrenVersion: Long, created: DateTime, modifiedData: DateTime, modifiedChildren: DateTime)

