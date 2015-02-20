package com.syntaxjockey.smr.world

import akka.util.ByteString
import com.syntaxjockey.smr.command.{InvalidPathException, RootModification}
import org.joda.time.DateTime
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.util.{Failure, Success}

class EphemeralWorldSpec extends WordSpec with ShouldMatchers {
  import com.syntaxjockey.smr.world.PathConversions._

  "An EphemeralWorld" should {

    val timestamp = DateTime.now()

    val root = Node("", ByteString.empty, Stat(0, 3, 1, 1, timestamp, timestamp, timestamp, 0), Set("foo", "bar", "baz"))
    val foo = Node("foo", ByteString.empty, Stat(0, 0, 2, 2, timestamp, timestamp, timestamp, 0), Set.empty)
    val bar = Node("bar", ByteString("hello, world"), Stat(12, 1, 1, 1, timestamp, timestamp, timestamp, 0), Set("qux"))
    val baz = Node("baz", ByteString.empty, Stat(0, 0, 1, 1, timestamp, timestamp, timestamp, 0), Set.empty)
    val qux = Node("qux", ByteString.empty, Stat(0, 0, 1, 1, timestamp, timestamp, timestamp, 0), Set.empty)

    def makeWorld = {
      val world = new EphemeralWorld
      world inTransaction { txn =>
        world.putNode(txn, "/", root)
        world.putNode(txn, "/foo", foo)
        world.putNode(txn, "/bar", bar)
        world.putNode(txn, "/baz", baz)
        world.putNode(txn, "/bar/qux", qux)
      }
      world
    }

    "find the root node" in {
      val world = makeWorld
      world.findNode("/") shouldEqual Some(root)
    }

    "find an intermediate node" in {
      val world = makeWorld
      world.findNode("/bar") shouldEqual Some(bar)
    }

    "find a leaf node" in {
      val world = makeWorld
      world.findNode("/bar/qux") shouldEqual Some(qux)
    }

    "return None when trying to find a node which doesn't exist" in {
      val world = makeWorld
      world.findNode("/foo/baz") shouldEqual None
    }

    "return true if a root node exists" in {
      val world = makeWorld
      world.checkNode("/") should be(true)
    }

    "return true if an intermediate node exists" in {
      val world = makeWorld
      world.checkNode("/bar") should be(true)
    }

    "return true if a leaf node exists" in {
      val world = makeWorld
      world.checkNode("/bar/qux") should be(true)
    }

    "return false if a node doesn't exist" in {
      val world = makeWorld
      world.checkNode("/foo/baz") should be(false)
    }

    "create a new node" in {
      val world = makeWorld
      val ctime = DateTime.now()
      val version = world.version
      val result = world.createNode("/foo/new", ByteString("test data"), ctime, isSequential = false)
      world.version should equal(version + 1)
      result match {
        case Failure(ex) =>
          fail("create /foo/new failed: " + ex)
        case Success(node) =>
          node.name should equal("new")
          node.data shouldEqual ByteString("test data")
          node.children should equal(Set.empty)
          node.stat.dataVersion should equal(version + 1)
          node.stat.childrenVersion should equal(version + 1)
      }
    }

    "create a new sequential node" in {
      val world = makeWorld
      val ctime = DateTime.now()
      val version = world.version
      val result = world.createNode("/foo/seq", ByteString("test data"), ctime, isSequential = true)
      world.version should equal(version + 1)
      result match {
        case Failure(ex) =>
          fail("create /foo/seq-00000001 failed: " + ex)
        case Success(node) =>
          node.name should equal("seq-00000001")
          node.data shouldEqual ByteString("test data")
          node.children should equal(Set.empty)
          node.stat.dataVersion should equal(version + 1)
          node.stat.childrenVersion should equal(version + 1)
          world.getNode("/foo").get.stat.seqCounter should equal(1)
      }
    }

//    "return failure when attempting to create a sequential node on a parent whose seqCounter is Int.maxValue" in {
//      val ns = Namespace("ns", 2, timestamp, Node("", ByteString.empty, Stat(0, 0, 1, 1, timestamp, timestamp, timestamp, Int.MaxValue), Map.empty))
//      val ctime = DateTime.now()
//      ns.create("/seq", ByteString("test data"), ns.version + 1, ctime, isSequential = true) match {
//        case Success(_) => fail("creating sequential node should return Failure")
//        case Failure(ex: SequentialOverflow) => // success
//        case Failure(ex) => fail("node creation failed with unexpected exception: {}", ex)
//      }
//    }

    "return failure when attempting to create a node whose parent doesn't exist" in {
      val world = makeWorld
      val ctime = DateTime.now()
      world.createNode("/foo/bar/new", ByteString("test data"), ctime, isSequential = false) match {
        case Success(_) => fail("creating node whose parent doesn't exist should return Failure")
        case Failure(ex: InvalidPathException) => // success
        case Failure(ex) => fail("node creation failed with unexpected exception: " + ex)
      }
    }

    "return failure when attempting to create a node which already exists" in {
      val world = makeWorld
      val ctime = DateTime.now()
      world.createNode("/foo", ByteString("test data"), ctime, isSequential = false) match {
        case Success(_) => fail("creating node which already exists should return Failure")
        case Failure(ex: InvalidPathException) => // success
        case Failure(ex) => fail("node creation failed with unexpected exception: " + ex)
      }
    }

    "delete an existing node" in {
      val world = makeWorld
      val mtime = DateTime.now()
      val version = world.version
      world.removeNode("/foo", None, mtime) match {
        case Success(()) =>
          world.version should equal(version + 1)
          world.checkNode("/foo") should equal(false)
        case Failure(ex) =>
          fail("remove /foo failed: " + ex)
      }
    }

    "delete an existing node with a version specified" in {
      val world = makeWorld
      val mtime = DateTime.now()
      val version = world.version
      world.removeNode("/foo", Some(2), mtime) match {
        case Success(()) =>
          world.version should equal(version + 1)
          world.checkNode("/foo") should equal(false)
        case Failure(ex) =>
          fail("remove /foo failed: " + ex)
      }
    }

    "return failure when attempting to delete a node which doesn't exist" in {
      val world = makeWorld
      val mtime = DateTime.now()
      world.removeNode("/foo/bar/new", None, mtime) match {
        case Success(()) => fail("deleting node which doesn't exist should return Failure")
        case Failure(ex: InvalidPathException) => // success
        case Failure(ex) => fail("remove /foo/bar/new failed with unexpected exception: " + ex)
      }
    }

    "return failure when attempting to delete the root node" in {
      val world = makeWorld
      val mtime = DateTime.now()
      world.removeNode("/", None, mtime) match {
        case Success(()) => fail("deleting the root node should return Failure")
        case Failure(ex: RootModification) => // success
        case Failure(ex) => fail("remove / failed with unexpected exception: " + ex)
      }
    }
  }
}
