package com.syntaxjockey.smr.namespace

import akka.util.ByteString
import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr.{WorldState, Result, Command}
import org.joda.time.DateTime

/**
 * 
 */
sealed trait NamespaceCommand extends Command

case class CreateNode(namespace: String, path: Path, data: ByteString, ctime: DateTime) extends NamespaceCommand {
  def apply(world: WorldState): Try[NodeResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        val cversion = world.version + 1
        ns.create(path, data, cversion, ctime) match {
          case Success(updated) =>
            Success(NodeResult(updated.get(path), this, WorldState(cversion, world.namespaces + (namespace -> updated))))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class DeleteNode(namespace: String, path: Path, version: Option[Long], mtime: DateTime) extends NamespaceCommand {
  def apply(world: WorldState): Try[EmptyResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        val mversion = world.version + 1
        ns.delete(path, version, mversion, mtime) match {
          case Success(updated) =>
            Success(EmptyResult(this, WorldState(mversion, world.namespaces + (namespace -> updated))))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class NodeExists(namespace: String, path: Path) extends NamespaceCommand {
  def apply(world: WorldState): Try[ExistsResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(ExistsResult(Some(node.stat), this, world))
          case None =>
            Success(ExistsResult(None, this, world))
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class GetNodeData(namespace: String, path: Path) extends NamespaceCommand {
  def apply(world: WorldState): Try[NodeResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(NodeResult(node, this, world))
          case None =>
            Failure(new InvalidPathException("path %s doesn't exist".format(path)))
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class SetNodeData(namespace: String, path: Path, data: ByteString, version: Option[Long], mtime: DateTime) extends NamespaceCommand {
  def apply(world: WorldState): Try[NodeResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        val mversion = world.version + 1
        ns.update(path, data, version, mversion, mtime) match {
          case Success(updated) =>
            Success(NodeResult(updated.get(path), this, WorldState(mversion, world.namespaces + (namespace -> updated))))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class GetNodeChildren(namespace: String, path: Path) extends NamespaceCommand {
  def apply(world: WorldState): Try[StatAndChildrenResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(StatAndChildrenResult(node.stat, node.children.keys.map(path :+ _), this, world))
          case None =>
            Failure(new InvalidPathException("path %s doesn't exist".format(path)))
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

/**
 *
 */
sealed trait NamespaceResult extends Result {
  val world: WorldState
  val op: NamespaceCommand
}

/*
 * Namespace operation result classes
 */
case class EmptyResult(op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class ExistsResult(stat: Option[Stat], op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class NodeResult(node: Node, op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class StatAndChildrenResult(stat: Stat, children: Iterable[Path], op: NamespaceCommand, world: WorldState) extends NamespaceResult
