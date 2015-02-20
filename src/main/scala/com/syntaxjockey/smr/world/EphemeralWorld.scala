package com.syntaxjockey.smr.world

import akka.util.ByteString
import com.syntaxjockey.smr.command.InvalidPathException
import org.joda.time.DateTime
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * An in-memory implementation of the world state.
 *
 * NOTE: EphemeralWorld does not persist to stable storage!  This implementation
 * should only be used for testing.
 */
class EphemeralWorld extends World {

  private var _configurations: Vector[Configuration] = Vector.empty
  private var _version: Long = 0
  private var _namespace = {
    val namespace = new mutable.HashMap[Path,Node]()
    val stat = Stat(0, 0, 0, 0, new DateTime(0), new DateTime(0), new DateTime(0), 0)
    val node = Node("", ByteString.empty, stat, Set.empty)
    namespace.put(Path.root, node)
    namespace
  }

  private class EphemeralTxn(proposedVersion: Long) extends Transaction {

    val proposedUpdates = new mutable.HashMap[Path,Node]()

    def version = proposedVersion

    def commit(): Unit = {
      proposedUpdates.clear()
      _version = proposedVersion
    }

    /* undo any mutations */
    def rollback(): Unit = {
      proposedUpdates.foreach { case (path,node) => _namespace.put(path, node) }
    }
  }

  //
  //
  //

  def version: Long = _version

  def configurations: Vector[Configuration] = _configurations
  def appendConfiguration(configuration: Configuration) = _configurations = _configurations :+ configuration
  def dropConfiguration() = _configurations = _configurations.tail

  override def createTransaction: Transaction = new EphemeralTxn(version + 1)

  override def checkNode(path: Path): Boolean = _namespace.contains(path)

  override def getNode(path: Path): Try[Node] = _namespace.get(path) match {
    case Some(node) => Success(node)
    case None => Failure(new InvalidPathException("node doesn't exist"))
  }

  override def getStat(path: Path): Try[Stat] = _namespace.get(path) match {
    case Some(node) => Success(node.stat)
    case None => Failure(new InvalidPathException("node doesn't exist"))
  }

  override def getData(path: Path): Try[ByteString] = _namespace.get(path) match {
    case Some(node) => Success(node.data)
    case None => Failure(new InvalidPathException("node doesn't exist"))
  }

  override def putNode(transaction: Transaction, path: Path, node: Node): Try[Unit] = transaction match {
    case txn: EphemeralTxn =>
      _namespace.get(path).foreach { prev => txn.proposedUpdates.put(path, prev) }
      Success(_namespace.put(path, node))
    case unknown =>
      Failure(new IllegalArgumentException("wrong transaction type"))
  }

  override def putStat(transaction: Transaction, path: Path, stat: Stat): Try[Unit] = transaction match {
    case txn: EphemeralTxn =>
      _namespace.get(path) match {
        case Some(node) => Success(_namespace.put(path, node.copy(stat = stat)))
        case None => Failure(new InvalidPathException("node doesn't exist"))
      }
    case unknown =>
      Failure(new IllegalArgumentException("wrong transaction type"))
  }

  override def deleteNode(transaction: Transaction, path: Path): Try[Unit] = transaction match {
    case txn: EphemeralTxn =>
      _namespace.remove(path) match {
        case Some(node) => Success()
        case None => Failure(new InvalidPathException("node doesn't exist"))
      }
    case unknown =>
      Failure(new IllegalArgumentException("wrong transaction type"))
  }
}

