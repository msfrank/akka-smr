package com.syntaxjockey.smr.namespace

import com.syntaxjockey.smr._
import scala.util.{Failure, Success, Try}
import com.syntaxjockey.smr.world.WorldState

sealed trait NamespaceCommand extends Command

case class CreateNamespace(name: String) extends NamespaceCommand with MutationCommand {
  def transform(world: WorldState): Try[WorldStateResult] = if (!world.namespaces.contains(name)) {
    val transformed = WorldState(world.version + 1, world.namespaces + (name -> Namespace(name)), world.config)
    Success(WorldStateResult(transformed, CreateNamespaceResult(name, this)))
  } else Failure(new NamespaceExists(name))
}

case class DeleteNamespace(name: String) extends NamespaceCommand with MutationCommand {
  def transform(world: WorldState): Try[WorldStateResult] = if (world.namespaces.contains(name)) {
    val transformed = WorldState(world.version + 1, world.namespaces - name, world.config)
    Success(WorldStateResult(transformed, DeleteNamespaceResult(name, this)))
  } else Failure(new NamespaceAbsent(name))
}

case class CreateNamespaceResult(name: String, op: CreateNamespace) extends Result

case class DeleteNamespaceResult(name: String, op: DeleteNamespace) extends Result