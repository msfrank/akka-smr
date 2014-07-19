package com.syntaxjockey.smr.raft

import akka.actor.ActorRef
import com.syntaxjockey.smr.Command

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
}

/**
 * An in-memory implementation of the replication log.
 *
 * NOTE: InMemoryLog does not persist the log to stable storage!  This implementation
 * should only be used for testing.
 */
class InMemoryLog(initial: Iterable[LogEntry]) extends Log {
  private[this] var log = initial.toVector

  def apply(index: Int): LogEntry = log(index)
  def last: LogEntry = log.last
  def lastOption: Option[LogEntry] = log.lastOption
  def drop(after: Int): Iterable[LogEntry] = log.drop(after)
  def slice(from: Int, until: Int): Iterable[LogEntry] = log.slice(from, until)
  def isEmpty: Boolean = log.isEmpty
  def isDefinedAt(index: Int): Boolean = log.isDefinedAt(index)
  def length: Int = log.length

  def append(entry: LogEntry): Unit = log = log :+ entry
  def append(entries: Iterable[LogEntry]): Unit = log = log ++ entries

  def removeAfter(index: Int): Iterable[LogEntry] = {
    val removed = log.drop(index)
    log = log.take(index)
    removed
  }
}
