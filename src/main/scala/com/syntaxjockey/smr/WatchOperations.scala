package com.syntaxjockey.smr

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.syntaxjockey.smr.namespace.NamespacePath
import scala.util.Try

trait WatchOperations extends Actor with ActorLogging {

  var watches: Map[NamespacePath,Set[ActorRef]]

  /**
   * if the specified command is a Watch, then update the watches map, otherwise
   * do nothing.
   */
  def updateWatches(command: Command): Command = command match {
    case Watch(watched, observer) =>
      val nspath = watched.watchPath()
      watches = watches + (nspath -> (watches.getOrElse(nspath, Set.empty) + observer))
      log.debug("added watch on {}", nspath)
      watched
    case notwatched => notwatched
  }

  /**
   * if the specified result is a WatchResult, then scan the watches map, remove all
   * matching watches, and return a map of notification maps, otherwise return an empty
   * map.
   */
  def notifyWatches(result: Result): Map[ActorRef,Map[NamespacePath,Notification]] = result match {
    case watchResult: WatchResult =>
      var notifications = Map.empty[ActorRef,Map[NamespacePath,Notification]]
      val mutations = watchResult.notifyPath()
      log.debug("mutation resulted in notification events {}", mutations.mkString(", "))
      mutations.foreach { mutation =>
        watches.get(mutation.nspath) match {
          case Some(watchers) =>
            watches = watches - mutation.nspath
            watchers.foreach { watcher =>
              val notification = watcher -> (notifications.getOrElse(watcher, Map.empty) + (mutation.nspath -> mutation))
              notifications = notifications + notification
            }
          case None => // do nothing
        }
      }
      notifications
    case _ => Map.empty
  }
}

/**
 * A command which can be watched.
 */
trait WatchableCommand extends Command {
  def watchPath(): NamespacePath
}

/**
 * A result which may have mutations.
 */
trait WatchResult extends Result {
  def notifyPath(): Vector[Notification]
}

/**
 * Set a watch on the specified command.
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
 *
 */
case class NotificationMap(notifications: Map[NamespacePath,Notification])

/**
 *
 */
case class NotificationResult(result: Result, notifications: NotificationMap) extends Result
