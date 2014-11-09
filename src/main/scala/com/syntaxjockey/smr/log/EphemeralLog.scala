package com.syntaxjockey.smr.log

/**
 * An in-memory implementation of the replication log.
 *
 * NOTE: EphemeralLog does not persist the log to stable storage!  This implementation
 * should only be used for testing.
 */
class EphemeralLog(initial: Iterable[LogEntry]) extends Log {
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
  def close(): Unit = { }
}