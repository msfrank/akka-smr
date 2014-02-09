package com.syntaxjockey.smr

import akka.actor.ActorRef
import scala.util.{Failure, Try}

import com.syntaxjockey.smr.namespace.{NamespacePath, Path}

/**
 *  marker trait for a command operation.
 */
trait Command {

  /**
   * Apply the command to the world state, returning a new world state if
   * successful, otherwise an Exception.
   */
  def apply(world: WorldState): Try[WorldStateResult]
}

/**
 *
 */
trait WatchableCommand extends Command {
  def watchPath(): NamespacePath
}

/**
 *
 */
case class Watch(command: WatchableCommand, observer: ActorRef) extends Command {
  def apply(world: WorldState): Try[WorldStateResult] = command.apply(world)
}

/**
 *
 */
case class Notification(nspath: NamespacePath, event: Notification.NotificationEvent)

object Notification {
  sealed trait NotificationEvent
  case object NodeCreatedEvent extends NotificationEvent
  case object NodeChildrenChangedEvent extends NotificationEvent
  case object NodeDataChangedEvent extends NotificationEvent
  case object NodeDeletedEvent extends NotificationEvent
}
/**
 * marker trait for an operation result.
 */
trait Result


trait MutationResult extends Result {
  def notifyPath(): Vector[Notification]
}

/**
 *
 */
class CommandFailed(cause: Throwable, val command: Command) extends Exception("Command failed", cause) with Result

/**
 *
 */
case class NotificationMap(notifications: Map[NamespacePath,Notification])

/**
 *
 */
case class NotificationResult(result: Result, notifications: NotificationMap) extends Result

/**
 *
 */
case class WorldStateResult(world: WorldState, result: Result)
