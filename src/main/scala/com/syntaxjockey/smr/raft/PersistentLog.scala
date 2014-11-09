package com.syntaxjockey.smr.raft

import java.io.{EOFException, ObjectOutputStream, ObjectInputStream}
import java.nio.channels.Channels
import java.nio.file._
import java.util.zip.CRC32

import akka.serialization.Serialization
import org.slf4j.LoggerFactory

import scala.util.{Success, Failure}

/**
 * An implementation of the replication log which persists to local storage.
 */
class PersistentLog(val logDirectory: Path, serialization: Serialization) extends Log {

  // config
  val logger = LoggerFactory.getLogger(classOf[PersistentLog])
  val rotationModulo = 1024

  // state
  private[this] var log = Vector.empty[LogEntry]
  private[this] var lsn: Long = 0
  private[this] var activeSegment: Option[ObjectOutputStream] = None

  private[this] val filesystem = FileSystems.getDefault

  /* if log directory doesn't exist, then create it */
  if (!Files.isDirectory(logDirectory))
    Files.createDirectory(logDirectory)

  /* if snapshot exists, then apply it first */
  private[this] val snapshotPath = filesystem.getPath(logDirectory.toString, "snapshot")
  if (Files.exists(snapshotPath)) {
    logger.debug("loading snapshot from " + snapshotPath.toString)
    val snapshotChannel = Files.newByteChannel(snapshotPath, StandardOpenOption.READ)
    val inputStream = new ObjectInputStream(Channels.newInputStream(snapshotChannel))
    try {
      val version = inputStream.readInt()
      if (version != PersistentLog.VERSION)
        throw new Exception("snapshot file format version mismatch")
      val size = inputStream.readLong()
      val id = inputStream.readInt()
      val bytes = new Array[Byte](size.toInt)
      inputStream.readFully(bytes)
      val checksum = new CRC32()
      checksum.update(bytes)
      val crc = inputStream.readLong()
      if (checksum.getValue != crc)
        throw new Exception("CRC32 checksum mismatch")
      serialization.deserialize(bytes, id, Some(classOf[PersistentLogSnapshot])) match {
        case Success(snapshot) =>
          log = snapshot.entries
          lsn = snapshot.lsn
          logger.debug("fast-forward to LSN " + lsn)
        case Failure(ex) =>
          throw ex
      }
    } finally {
      inputStream.close()
    }
  }

  /* if any log segments exist, then apply each segment to the log */
  private[this] val segmentPaths = {
    val directoryStream: DirectoryStream[Path] = Files.newDirectoryStream(logDirectory, "segment.*")
    var segments = Vector.empty[Path]
    import scala.language.implicitConversions
    import scala.collection.JavaConversions._
    for (dir <- directoryStream.iterator())
      segments = segments :+ dir
    segments.sorted
  }
  segmentPaths.foreach { segmentPath =>
    logger.debug("loading segment from " + segmentPath.toString)
    val segmentChannel = Files.newByteChannel(segmentPath, StandardOpenOption.READ)
    val inputStream = new ObjectInputStream(Channels.newInputStream(segmentChannel))
    try {
      val version = inputStream.readInt()
      if (version != PersistentLog.VERSION)
        throw new Exception("snapshot file format version mismatch")
      while (true) {
        val size = inputStream.readLong()
        val id = inputStream.readInt()
        val bytes = new Array[Byte](size.toInt)
        inputStream.readFully(bytes)
        val checksum = new CRC32()
        checksum.update(bytes)
        val crc = inputStream.readLong()
        if (checksum.getValue != crc)
          throw new Exception("CRC32 checksum mismatch")
        serialization.deserialize(bytes, id, Some(classOf[PersistentLogOperation])) match {
          case Success(op) =>
            mutate(op)
          case Failure(ex) =>
            throw ex
        }
      }
    } catch {
      case ex: EOFException =>
    } finally {
      inputStream.close()
    }
  }

  /**
   * Apply the specified operation to the in-memory log.
   */
  private[this] def mutate(op: PersistentLogOperation): Unit = {
    if (op.lsn > 1 && lsn == 0)
      throw new Exception("expected lsn 1, found lsn " + op.lsn)
    if (op.lsn != lsn + 1)
      throw new Exception("expected lsn %d, found lsn %d".format(lsn + 1, op.lsn))
    op match {
      case PersistentLogAppend(_, entries) =>
        log = log ++ entries
        lsn = op.lsn
      case PersistentLogRemove(_, after) =>
        log = log.take(after)
        lsn = op.lsn
    }
    logger.debug("applied operation " + op.toString)
  }

  /**
   * Persist the specified operation to stable storage.
   */
  private[this] def persist(op: PersistentLogOperation): Unit = {
    val outputStream = activeSegment.getOrElse {
      val segmentPath = filesystem.getPath(logDirectory.toString, "segment.%016x".format(op.lsn))
      val segmentChannel = Files.newByteChannel(segmentPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      val outputStream = new ObjectOutputStream(Channels.newOutputStream(segmentChannel))
      activeSegment = Some(outputStream)
      outputStream.writeInt(PersistentLog.VERSION)
      outputStream
    }
    val serializer = serialization.findSerializerFor(op)
    val id = serializer.identifier
    val bytes = serializer.toBinary(op)
    val size = bytes.length
    val checksum = new CRC32()
    checksum.update(bytes)
    outputStream.writeLong(size)
    outputStream.writeInt(id)
    outputStream.write(bytes)
    outputStream.writeLong(checksum.getValue)
    logger.debug("persisted LSN " + op.lsn)
  }

  def close(): Unit = {
    activeSegment.foreach(_.close())
    activeSegment = None
  }

  def append(entry: LogEntry): Unit = {
    val op = PersistentLogAppend(lsn + 1, Vector(entry))
    mutate(op)
    persist(op)
  }

  def append(entries: Iterable[LogEntry]): Unit = {
    val op = PersistentLogAppend(lsn + 1, entries.toVector)
    mutate(op)
    persist(op)
  }

  def removeAfter(index: Int): Iterable[LogEntry] = {
    val removed = log.drop(index)
    val op = PersistentLogRemove(lsn + 1, index)
    mutate(op)
    persist(op)
    removed
  }

  def apply(index: Int): LogEntry = log(index)
  def last: LogEntry = log.last
  def lastOption: Option[LogEntry] = log.lastOption
  def drop(after: Int): Iterable[LogEntry] = log.drop(after)
  def slice(from: Int, until: Int): Iterable[LogEntry] = log.slice(from, until)
  def isEmpty: Boolean = log.isEmpty
  def isDefinedAt(index: Int): Boolean = log.isDefinedAt(index)
  def length: Int = log.length
}

object PersistentLog {
  val VERSION = 1
}

sealed trait PersistentLogOperation extends Serializable { val lsn: Long }
case class PersistentLogAppend(lsn: Long, entries: Vector[LogEntry]) extends PersistentLogOperation
case class PersistentLogRemove(lsn: Long, after: Int) extends PersistentLogOperation

case class PersistentLogSnapshot(lsn: Long, entries: Vector[LogEntry]) extends Serializable
