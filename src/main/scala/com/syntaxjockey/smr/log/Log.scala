package com.syntaxjockey.smr.log

import akka.actor.ActorRef
import com.syntaxjockey.smr.command.Command

/**
 * A single entry in the RAFT replication log.
 */
case class LogEntry(command: Command, caller: ActorRef, index: Int, term: Int)

/**
 * The RAFT replication log interface.
 */
trait Log {
  def apply(index: Int): LogEntry
  def last: LogEntry
  def lastOption: Option[LogEntry]
  def drop(after: Int): Iterable[LogEntry]
  def slice(from: Int, until: Int): Iterable[LogEntry]
  def isEmpty: Boolean
  def isDefinedAt(index: Int): Boolean
  def length: Int
  def append(entries: Iterable[LogEntry]): Unit
  def append(entry: LogEntry): Unit
  def removeAfter(index: Int): Iterable[LogEntry]
  def close(): Unit
}

