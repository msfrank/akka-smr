package com.syntaxjockey.smr

import scala.util.Try

/**
 *  marker trait for a command operation.
 */
trait Command {

  /**
   * Apply the command to the world state, returning a new world state if
   * successful, otherwise an Exception.
   */
  def apply(world: WorldState): Try[Result]
}

/**
 * marker trait for an operation result.
 */
trait Result {
  val world: WorldState
}

/**
 *
 */
class CommandFailed(cause: Throwable, val command: Command, val world: WorldState) extends Exception("Command failed", cause) with Result
