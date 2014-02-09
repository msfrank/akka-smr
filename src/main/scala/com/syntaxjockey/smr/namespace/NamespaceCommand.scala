package com.syntaxjockey.smr.namespace

import com.syntaxjockey.smr.{Result, WorldStateResult, WorldState, Command}
import scala.util.{Failure, Success, Try}

sealed trait NamespaceCommand extends Command

case class CreateNamespace(name: String) extends NamespaceCommand {
  def apply(world: WorldState): Try[WorldStateResult] = if (!world.namespaces.contains(name)) {
    Success(WorldStateResult(WorldState(world.version + 1, world.namespaces + (name -> Namespace(name))), CreateNamespaceResult(name, this)))
  } else Failure(new NamespaceExists(name))
}

case class DeleteNamespace(name: String) extends NamespaceCommand {
  def apply(world: WorldState): Try[WorldStateResult] = if (world.namespaces.contains(name)) {
    Success(WorldStateResult(WorldState(world.version + 1, world.namespaces - name), DeleteNamespaceResult(name, this)))
  } else Failure(new NamespaceAbsent(name))
}

case class CreateNamespaceResult(name: String, op: CreateNamespace) extends Result

case class DeleteNamespaceResult(name: String, op: DeleteNamespace) extends Result