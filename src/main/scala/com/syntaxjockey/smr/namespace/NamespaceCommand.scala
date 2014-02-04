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
  def apply(world: WorldState): Try[PathResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.create(path, data, ctime) match {
          case Success(updated) =>
            Success(PathResult(path, this, WorldState(updated.version, world.namespaces + (namespace -> updated))))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class DeleteNode(namespace: String, path: Path, version: Option[Long], mtime: DateTime) extends NamespaceCommand {
  def apply(world: WorldState): Try[NoneResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.delete(path, version, mtime) match {
          case Success(updated) =>
            Success(NoneResult(this, WorldState(updated.version, world.namespaces + (namespace -> updated))))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

//case class NodeExists(namespace: String, path: Path) extends NamespaceCommand

case class GetNodeData(namespace: String, path: Path) extends NamespaceCommand {
  def apply(world: WorldState): Try[ChildrenAndStatResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(ChildrenAndStatResult(node.children.keys.toVector.map(path :+ _), node.stat, this, world))
          case None =>
            Failure(new InvalidPathException("path %s doesn't exist".format(path)))
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

case class GetNodeChildren(namespace: String, path: Path) extends NamespaceCommand {
  def apply(world: WorldState): Try[ChildrenAndStatResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(ChildrenAndStatResult(node.children.keys.toVector.map(path :+ _), node.stat, this, world))
          case None =>
            Failure(new InvalidPathException("path %s doesn't exist".format(path)))
        }
      case None =>
        Failure(new Exception("namespace %s doesn't exist".format(namespace)))
    }
  }
}

//case class SetNodeData(path: Path, data: ByteString, version: Option[Long]) extends NamespaceOperations

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
case class NoneResult(op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class PathResult(path: Path, op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class StatResult(stat: Stat, op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class PathAndStatResult(path: Path, stat: Stat, op: NamespaceCommand, world: WorldState) extends NamespaceResult
case class ChildrenAndStatResult(children: Vector[Path], stat: Stat, op: NamespaceCommand, world: WorldState) extends NamespaceResult
