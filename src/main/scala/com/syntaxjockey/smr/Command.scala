package com.syntaxjockey.smr

/**
 *  marker trait for a command operation.
 */
trait Command

/**
 * marker trait for an operation result.
 */
trait Result

/**
 *
 */
class CommandFailed(cause: Throwable, val command: Command) extends Exception("Command failed", cause) with Result

/**
 *
 */
case object RSMStatus extends Command