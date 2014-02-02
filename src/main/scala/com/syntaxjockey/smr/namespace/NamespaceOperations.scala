package com.syntaxjockey.smr.namespace

import akka.util.ByteString
import org.joda.time.DateTime

sealed trait NamespaceOperations {
  val timestamp = DateTime.now()
  def merge(namespace: Namespace): NamespaceOperationResult
}

case class CreateNode(path: Path, data: ByteString) extends NamespaceOperations {
  def merge(namespace: Namespace): NamespaceOperationResult = PathResult(path, this, namespace.create(path, data))
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

/*
 * Namespace operation result classes
 */
sealed trait NamespaceOperationResult {
  val namespace: Namespace
  val op: NamespaceOperations
}

case class PathResult(path: Path, op: NamespaceOperations, namespace: Namespace) extends NamespaceOperationResult
case class StatResult(stat: Stat, op: NamespaceOperations, namespace: Namespace) extends NamespaceOperationResult
case class PathAndStatResult(path: Path, stat: Stat, op: NamespaceOperations, namespace: Namespace) extends NamespaceOperationResult
case class ChildrenResult(children: Vector[Path], op: NamespaceOperations, namespace: Namespace) extends NamespaceOperationResult
case class ChildrenAndStatResult(children: Vector[Path], stat: Stat, op: NamespaceOperations, namespace: Namespace) extends NamespaceOperationResult
