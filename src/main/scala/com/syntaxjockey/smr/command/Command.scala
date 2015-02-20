package com.syntaxjockey.smr.command

import scala.util.{Success, Try}

import com.syntaxjockey.smr.world.{Path, World}
import com.syntaxjockey.smr.Notification

/**
 *  marker trait for a command operation.
 */
trait Command {
  /**
   * Apply the command to the world state, returning a new world state if
   * successful, otherwise an Exception.
   */
  def apply(world: World): Try[Response]
}

/**
 * marker trait for an operation result.
 */
trait Result

/**
 * A failure result indicating the specified command failed.
 */
class CommandFailed(cause: Throwable, val command: Command) extends Exception("Command failed", cause) with Result

/**
 * Wraps the result of a command along with the transformed world state and
 * any notification side-effects.
 */
case class Response(world: World, result: Result, notifications: Map[Path,Notification] = Map.empty)

/**
 * Ping command, simply returns PongResult.  This is useful for testing that
 * the cluster is available and processing commands.  The correlationId is
 * optional, but if specified is returned in the PongResult.
 */
case class PingCommand(correlationId: Option[Any] = None) extends Command {
  def apply(world: World): Try[Response] = Success(Response(world, PongResult(correlationId)))
}

/**
 * The result of a PingCommand.
 */
case class PongResult(correlationId: Option[Any]) extends Result
