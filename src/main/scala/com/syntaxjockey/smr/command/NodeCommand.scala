package com.syntaxjockey.smr.command

import akka.util.ByteString
import org.joda.time.DateTime
import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr._
import com.syntaxjockey.smr.world._

/**
 * 
 */
sealed trait NodeCommand extends Command

case class NodeExists(namespace: String, path: Path) extends NodeCommand with WatchableCommand {
  def apply(world: World): Try[Response] = {
    world.getStat(path) match {
      case Success(stat) =>
        Success(Response(world, ExistsResult(Some(stat), this)))
      case Failure(ex: InvalidPathException) =>
        Success(Response(world, ExistsResult(None, this)))
      case Failure(ex) =>
        Failure(ex)
    }
  }
  def watchPath() = NamespacePath(namespace, path)
}

case class GetNodeChildren(namespace: String, path: Path) extends NodeCommand with WatchableCommand {
  def apply(world: World): Try[Response] = {
    world.getNode(path) map { node =>
      Response(world, GetNodeChildrenResult(node.stat, node.children.map(path :+ _), this))
    }
  }
  def watchPath() = NamespacePath(namespace, path)
}

case class GetNodeData(namespace: String, path: Path) extends NodeCommand with WatchableCommand {
  def apply(world: World): Try[Response] = {
    world.getNode(path) map { node => Response(world, GetNodeDataResult(node.stat, node.data, this)) }
  }
  def watchPath() = NamespacePath(namespace, path)
}

case class CreateNode(namespace: String, path: Path, data: ByteString, ctime: DateTime, isSequential: Boolean = false) extends NodeCommand with MutationCommand {
  def transform(world: World): Try[Response] = {
    world.createNode(path, data, ctime, isSequential) map { node => Response(world, CreateNodeResult(path, this)) }
  }
}

case class SetNodeData(namespace: String, path: Path, data: ByteString, version: Option[Long], mtime: DateTime) extends NodeCommand with MutationCommand {
  def transform(world: World): Try[Response] = {
    world.modifyNode(path, data, version, mtime) map { stat => Response(world, SetNodeDataResult(stat, this)) }
  }
}

case class DeleteNode(namespace: String, path: Path, version: Option[Long], mtime: DateTime) extends NodeCommand with MutationCommand {
  def transform(world: World): Try[Response] = {
    world.removeNode(path, version, mtime).map { _ => Response(world, DeleteNodeResult(path, this)) }
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
