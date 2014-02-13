package com.syntaxjockey.smr.namespace

import akka.util.ByteString
import org.joda.time.DateTime
import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr._

/**
 * 
 */
sealed trait NodeCommand extends Command


case class NodeExists(namespace: String, path: Path) extends NodeCommand with WatchableCommand {
  def apply(world: WorldState): Try[WorldStateResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(WorldStateResult(world, ExistsResult(Some(node.stat), this)))
          case None =>
            Success(WorldStateResult(world, ExistsResult(None, this)))
        }
      case None =>
        Failure(new NamespaceAbsent(namespace))
    }
  }
  def watchPath() = NamespacePath(namespace, path)
}

case class GetNodeChildren(namespace: String, path: Path) extends NodeCommand with WatchableCommand {
  def apply(world: WorldState): Try[WorldStateResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(WorldStateResult(world, GetNodeChildrenResult(node.stat, node.children.keys.map(path :+ _), this)))
          case None =>
            Failure(new InvalidPathException("path %s doesn't exist".format(path)))
        }
      case None =>
        Failure(new NamespaceAbsent(namespace))
    }
  }
  def watchPath() = NamespacePath(namespace, path)
}

case class GetNodeData(namespace: String, path: Path) extends NodeCommand with WatchableCommand {
  def apply(world: WorldState): Try[WorldStateResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        ns.find(path) match {
          case Some(node) =>
            Success(WorldStateResult(world, GetNodeDataResult(node.stat, node.data, this)))
          case None =>
            Failure(new InvalidPathException("path %s doesn't exist".format(path)))
        }
      case None =>
        Failure(new NamespaceAbsent(namespace))
    }
  }
  def watchPath() = NamespacePath(namespace, path)
}

case class CreateNode(namespace: String, path: Path, data: ByteString, ctime: DateTime) extends NodeCommand with MutationCommand {
  def transform(world: WorldState): Try[WorldStateResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        val cversion = world.version + 1
        ns.create(path, data, cversion, ctime) match {
          case Success(updated) =>
            val transformed = WorldState(cversion, world.namespaces + (namespace -> updated))
            Success(WorldStateResult(transformed, CreateNodeResult(path, this)))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new NamespaceAbsent(namespace))
    }
  }
}

case class SetNodeData(namespace: String, path: Path, data: ByteString, version: Option[Long], mtime: DateTime) extends NodeCommand with MutationCommand {
  def transform(world: WorldState): Try[WorldStateResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        val mversion = world.version + 1
        ns.update(path, data, version, mversion, mtime) match {
          case Success(updated) =>
            val transformed = WorldState(mversion, world.namespaces + (namespace -> updated))
            Success(WorldStateResult(transformed, SetNodeDataResult(ns.get(path).stat, this)))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new NamespaceAbsent(namespace))
    }
  }
}

case class DeleteNode(namespace: String, path: Path, version: Option[Long], mtime: DateTime) extends NodeCommand with MutationCommand {
  def transform(world: WorldState): Try[WorldStateResult] = {
    world.namespaces.get(namespace) match {
      case Some(ns) =>
        val mversion = world.version + 1
        ns.delete(path, version, mversion, mtime) match {
          case Success(updated) =>
            val transformed = WorldState(mversion, world.namespaces + (namespace -> updated))
            Success(WorldStateResult(transformed, DeleteNodeResult(path, this)))
          case Failure(ex) =>
            Failure(ex)
        }
      case None =>
        Failure(new NamespaceAbsent(namespace))
    }
  }
}

/**
 *
 */
sealed trait NodeResult extends Result {
  val op: NodeCommand
}

/*
 * Namespace operation result classes
 */
case class ExistsResult(stat: Option[Stat], op: NodeExists) extends NodeResult
case class GetNodeChildrenResult(stat: Stat, children: Iterable[Path], op: GetNodeChildren) extends NodeResult
case class GetNodeDataResult(stat: Stat, data: ByteString, op: GetNodeData) extends NodeResult

case class CreateNodeResult(path: Path, op: CreateNode) extends NodeResult with MutationResult {
  def notifyPath() = Vector(
    Notification(NamespacePath(op.namespace, op.path.init), Notification.NodeChildrenChangedEvent),
    Notification(NamespacePath(op.namespace, op.path), Notification.NodeCreatedEvent)
  )
}

case class SetNodeDataResult(stat: Stat, op: SetNodeData) extends NodeResult with MutationResult {
  def notifyPath() = Vector(Notification(NamespacePath(op.namespace, op.path), Notification.NodeDataChangedEvent))
}

case class DeleteNodeResult(path: Path, op: DeleteNode) extends NodeResult with MutationResult {
  def notifyPath() = Vector(
    Notification(NamespacePath(op.namespace, op.path.init), Notification.NodeChildrenChangedEvent),
    Notification(NamespacePath(op.namespace, op.path), Notification.NodeDeletedEvent)
  )
}
