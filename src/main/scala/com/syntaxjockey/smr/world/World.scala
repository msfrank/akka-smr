package com.syntaxjockey.smr.world

import akka.actor.ActorRef
import akka.util.ByteString
import org.joda.time.DateTime
import scala.util._

import com.syntaxjockey.smr.command._

/**
 * Contains the state of the entire world.
 */
trait World {

  /**
   * Return the current world version.
   */
  def version: Long

  /**
   * Return the current active configurations
   */
  def configurations: Vector[Configuration]

  /**
   * Return the set of processors (including the local processor) that are present
   * in every active configuration.
   */
  def processors: Set[ActorRef] = configurations.flatMap(_.peers).toSet

  /**
   * Append the specified configuration to the tail of the configurations list.
   */
  def appendConfiguration(configuration: Configuration): Unit

  /**
   * Remove the oldest configuration (the head) from the configurations list.
   */
  def dropConfiguration(): Unit

  /**
   * Return true if the specified path exists, otherwise false
   */
  def checkNode(path: Path): Boolean
  def getNode(path: Path): Try[Node]
  def getStat(path: Path): Try[Stat]
  def getData(path: Path): Try[ByteString]

  def createTransaction: Transaction
  def putNode(transaction: Transaction, path: Path, node: Node): Try[Unit]
  def putStat(transaction: Transaction, path: Path, stat: Stat): Try[Unit]
  def deleteNode(transaction: Transaction, path: Path): Try[Unit]

  /**
   * Return Some(node) at the specified path, or None if the node doesn't exist.
   */
  def findNode(path: Path): Option[Node] = getNode(path).toOption

  /**
   *
   */
  def inTransaction[T](f: (Transaction) => Try[T]): Try[T] = {
    val transaction = createTransaction
    val result = f(transaction)
    if (result.isSuccess) transaction.commit() else transaction.rollback()
    result
  }

  /**
   * Create a node at the specified path with the specified data.
   */
  def createNode(path: Path, data: ByteString, ctime: DateTime, isSequential: Boolean): Try[Node] = {
    if (path != Path.root) {
      getNode(path.dirpath).flatMap { case parent =>
        if (parent.children.contains(path.entryname))
          throw new InvalidPathException("node already exists")
        if (isSequential && parent.stat.seqCounter == Int.MaxValue)
          throw new SequentialOverflow()
        inTransaction { txn =>
          val updatedCounter = if (isSequential) parent.stat.seqCounter + 1 else parent.stat.seqCounter
          val name = if (isSequential) "%s-%08x".format(path.entryname, updatedCounter) else path.entryname
          val createdStat = Stat(data.length, 0, txn.version, txn.version, ctime, ctime, ctime, 0)
          val createdNode = Node(name, data, Stat(data.length, 0, txn.version, txn.version, ctime, ctime, ctime, 0), Set.empty)
          putNode(txn, path, createdNode).get
          var ancestorPath = path.dirpath
          val parentStat = parent.stat.copy(numChildren = parent.children.size + 1,
            childrenVersion = txn.version, modifiedChildren = ctime, seqCounter = updatedCounter)
          val parentNode = Node(parent.name, parent.data, parentStat, parent.children + name)
          putNode(txn, ancestorPath, parentNode).get
          while (ancestorPath.notRoot) {
            ancestorPath = ancestorPath.dirpath
            getStat(ancestorPath) map { case ancestorStat =>
              putStat(txn, ancestorPath, ancestorStat.copy(childrenVersion = txn.version, modifiedChildren = ctime)).get
            }
          }
          Success(createdNode)
        }
      }
    } else Failure(new RootModification())
  }

  /**
   * Modify the data for the specified node, which must exist.
   */
  def modifyNode(path: Path, data: ByteString, uversion: Option[Long], mtime: DateTime): Try[Stat] = {
    if (path != Path.root) {
      getNode(path).flatMap { case node =>
        if (uversion.isDefined && uversion.get != node.stat.dataVersion)
          throw new VersionMismatch(uversion.get, node.stat.dataVersion)
        inTransaction { txn =>
          val modifiedStat = node.stat.copy(dataLength = data.length, dataVersion = txn.version, modifiedData = mtime)
          val modifiedNode = Node(node.name, data, modifiedStat, node.children)
          putNode(txn, path, modifiedNode).get
          var ancestorPath = path.dirpath
          while (ancestorPath.notRoot) {
            ancestorPath = ancestorPath.dirpath
            getStat(ancestorPath) map { case ancestorStat =>
              putStat(txn, ancestorPath, ancestorStat.copy(childrenVersion = txn.version, modifiedChildren = mtime)).get
            }
          }
          Success(modifiedStat)
        }
      }
    }
    else Failure(new RootModification())
  }

  /**
   * Delete the node at the specified path, and all of its children.
   */
  def removeNode(path: Path, dversion: Option[Long], mtime: DateTime): Try[Unit] = {
    if (path != Path.root) {
      getNode(path).flatMap { case leaf =>
        if (dversion.isDefined && dversion.get != leaf.stat.dataVersion)
          throw new VersionMismatch(dversion.get, leaf.stat.dataVersion)
        inTransaction { txn =>
          deleteNode(txn, path).get
          val parentPath = path.dirpath
          val parent = getNode(parentPath).get
          val modifiedStat = parent.stat.copy(numChildren = parent.children.size - 1, childrenVersion = txn.version, modifiedChildren = mtime)
          val modifiedNode = Node(parent.name, parent.data, modifiedStat, parent.children - leaf.name)
          putNode(txn, parentPath, modifiedNode).get
          var ancestorPath = parentPath
          while (ancestorPath.notRoot) {
            ancestorPath = ancestorPath.dirpath
            getStat(ancestorPath) map { case ancestorStat =>
              putStat(txn, ancestorPath, ancestorStat.copy(childrenVersion = txn.version, modifiedChildren = mtime)).get
            }
          }
          Success(Unit)
        }
      }
    } else Failure(new RootModification())
  }

}

/**
 *
 */
case class Node(name: String, data: ByteString, stat: Stat, children: Set[String])

/**
 * Contains the metadata about a Node.
 *
 * @param dataLength Size of the Node data, in bytes.
 * @param numChildren Count of Node children.
 * @param dataVersion The version in which the Node data was most recently changed.
 * @param childrenVersion The version in which the count of Node children was  most recently changed.
 * @param created The timestamp when the Node was created (NOTE: this is supplied by the client!).
 * @param modifiedData The timestamp when the Node data was most recently changed (NOTE: this is supplied by the client!).
 * @param modifiedChildren The timestamp when the count of Node children was most recently changed (NOTE: this is supplied by the client!).
 * @param seqCounter The sequence counter.  Incremented every time a sequential child node is created.
 */
case class Stat(dataLength: Int,
                numChildren: Int,
                dataVersion: Long,
                childrenVersion: Long,
                created: DateTime,
                modifiedData: DateTime,
                modifiedChildren: DateTime,
                seqCounter: Int)

/**
 *
 */
trait Transaction {
  def version: Long
  def commit(): Unit
  def rollback(): Unit
}
