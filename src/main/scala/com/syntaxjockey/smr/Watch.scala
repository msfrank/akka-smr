package com.syntaxjockey.smr

import akka.actor.{ActorContext, ActorRef}
import scala.util.{Success, Try}

import com.syntaxjockey.smr.world._
import com.syntaxjockey.smr.command._

/**
 * A command which can be watched.
 */
trait WatchableCommand extends Command {
  /**
   * Returns the absolute path to the node which may be watched.
   */
  def watchPath(): NamespacePath
}

/**
 * Set a watch on the specified command.
 */
case class Watch(command: WatchableCommand, observer: ActorRef) {
  /**
   * Returns a copy of the watches Map with a watch added for the namespace path
   * returned from command.watchPath().
   */
  def updateWatches(watches: Map[NamespacePath,Set[ActorRef]]): Map[NamespacePath,Set[ActorRef]] = {
    val nspath = command.watchPath()
    watches + (nspath -> (watches.getOrElse(nspath, Set.empty) + observer))
  }
}

object Watch {
  /**
   * Set a watch using the implicit actor context self reference as the observer.
   */
  def apply(command: WatchableCommand)(implicit context: ActorContext): Watch = Watch(command, context.self)
}

/**
 * A command which mutates the world state.
 */
trait MutationCommand extends Command {

  /**
   * Mutate the specified world state, and return the result.
   */
  def transform(world: World): Try[Response]

  /**
   * if the specified result is a MutationResult, then update the notification map
   */
  def apply(world: World): Try[Response] = transform(world) match {
    case Success(Response(transformed, result: MutationResult, _notifications)) =>
      var notifications = _notifications
      result.notifyPath().foreach {
        // if there is no notification pending for this nspath, then add it
        case mutation if !notifications.contains(mutation.nspath) =>
          notifications = notifications + (mutation.nspath -> mutation)
        case _ => // otherwise do nothing, the client can't catch it anyways
      }
      Success(Response(transformed, result, notifications))
    case result => result
  }
}

/**
 * The result of a command which may have mutated the world state.
 */
trait MutationResult {
  /**
   * Return a vector of notifications that result from the mutation.
   */
  def notifyPath(): Vector[Notification]
}

/**
 * Contains the notification type for a namespace path.
 */
case class Notification(nspath: NamespacePath, event: Notification.NotificationEvent)

object Notification {
  sealed trait NotificationEvent
  case object NodeCreatedEvent extends NotificationEvent
  case object NodeChildrenChangedEvent extends NotificationEvent
  case object NodeDataChangedEvent extends NotificationEvent
  case object NodeDeletedEvent extends NotificationEvent
}
