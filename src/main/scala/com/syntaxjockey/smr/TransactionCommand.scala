package com.syntaxjockey.smr

import scala.util.{Success, Failure, Try}

import com.syntaxjockey.smr.namespace.NamespacePath

/**
 * Wraps one ore more mutation commands in a transaction, so either all of the mutations
 * will be applied successfully, or none will.  Note that TransactionCommand is not itself
 * a MutationCommand, so it's not possible (nor does it make sense) to nest transactions.
 */
case class TransactionCommand(commands: Vector[MutationCommand]) extends Command {
  def apply(world: WorldState): Try[WorldStateResult] = if (!commands.isEmpty) {
    var _world: WorldState = world
    var notifications: Map[NamespacePath,Notification] = Map.empty
    val results = commands.map { command =>
      command.apply(_world) match {
        case Success(WorldStateResult(transformed, result: MutationResult, _notifications)) =>
          _world = transformed
          result.notifyPath().foreach {
            // if there is no notification pending for this nspath, then add it
            case mutation if !notifications.contains(mutation.nspath) =>
              notifications = notifications + (mutation.nspath -> mutation)
            case _ => // otherwise do nothing, the client can't catch it anyways
          }
          result
        case Success(WorldStateResult(transformed, result, _)) =>
          _world = transformed
          result
        case f: Failure[WorldStateResult] =>
          return f  // short-circuit failure path
      }
    }
    Success(WorldStateResult(_world, TransactionResult(results, this), notifications))
  } else Failure(new IllegalArgumentException("Can't execute empty transaction"))
}

/**
 * Contains the results of each individual command in the transaction, in the same
 * order as is specified in the TransactionCommand.
 */
case class TransactionResult(results: Vector[Result], op: TransactionCommand) extends Result
