package com.syntaxjockey.smr.namespace

import akka.util.ByteString

import com.syntaxjockey.smr.{Result, Command}

/**
 * 
 */
sealed trait NamespaceCommand extends Command {
  def merge(namespace: Namespace): NamespaceResult
}

case class CreateNode(path: Path, data: ByteString) extends NamespaceCommand {
  def merge(namespace: Namespace): NamespaceResult = PathResult(path, this, namespace.create(path, data))
}

//case class DeleteNode(path: Path, version: Option[Long]) extends NamespaceOperations
//
//case class NodeExists(path: Path) extends NamespaceOperations
//
//case class GetNodeData(path: Path) extends NamespaceOperations
//
//case class GetNodeChildren(path: Path) extends NamespaceOperations
//
//case class SetNodeData(path: Path, data: ByteString, version: Option[Long]) extends NamespaceOperations

/**
 *
 */
sealed trait NamespaceResult extends Result {
  val namespace: Namespace
  val op: NamespaceCommand
}

/*
 * Namespace operation result classes
 */
case class PathResult(path: Path, op: NamespaceCommand, namespace: Namespace) extends NamespaceResult
case class StatResult(stat: Stat, op: NamespaceCommand, namespace: Namespace) extends NamespaceResult
case class PathAndStatResult(path: Path, stat: Stat, op: NamespaceCommand, namespace: Namespace) extends NamespaceResult
case class ChildrenResult(children: Vector[Path], op: NamespaceCommand, namespace: Namespace) extends NamespaceResult
case class ChildrenAndStatResult(children: Vector[Path], stat: Stat, op: NamespaceCommand, namespace: Namespace) extends NamespaceResult
