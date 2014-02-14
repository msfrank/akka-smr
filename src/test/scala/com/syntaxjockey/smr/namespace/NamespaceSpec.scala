package com.syntaxjockey.smr.namespace

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.joda.time.DateTime
import akka.util.ByteString
import scala.util.{Failure, Success}

class NamespaceSpec extends WordSpec with MustMatchers {
  import PathConversions._

  "A Namespace" must {

    val timestamp = DateTime.now()
    val foo = Node("foo", ByteString.empty, Stat(0, 0, 2, 2, timestamp, timestamp, timestamp, 0), Map.empty)
    val qux = Node("qux", ByteString.empty, Stat(0, 0, 1, 1, timestamp, timestamp, timestamp, 0), Map.empty)
    val baz = Node("baz", ByteString.empty, Stat(0, 0, 1, 1, timestamp, timestamp, timestamp, 0), Map.empty)
    val bar = Node("bar", ByteString("hello, world"), Stat(12, 1, 1, 1, timestamp, timestamp, timestamp, 0), Map("qux" -> qux))
    val root = Node("", ByteString.empty, Stat(0, 3, 1, 1, timestamp, timestamp, timestamp, 0), Map("foo" -> foo, "bar" -> bar, "baz" -> baz))
    val ns = Namespace("ns", 2, timestamp, root)

    "find the root node" in {
      ns.find("/") must be === Some(root)
    }

    "find an intermediate node" in {
      ns.find("/bar") must be === Some(bar)
    }

    "find a leaf node" in {
      ns.find("/bar/qux") must be === Some(qux)
    }

    "return None when trying to find a node which doesn't exist" in {
      ns.find("/foo/baz") must be(None)
    }

    "return true if a root node exists" in {
      ns.exists("/") must be(true)
    }

    "return true if an intermediate node exists" in {
      ns.exists("/bar") must be(true)
    }

    "return true if a leaf node exists" in {
      ns.exists("/bar/qux") must be(true)
    }

    "return false if a node doesn't exist" in {
      ns.exists("/foo/baz") must be(false)
    }

    "create a new node" in {
      val ctime = DateTime.now()
      val updated = ns.create("/foo/new", ByteString("test data"), ns.version + 1, ctime, isSequential = false).get
      updated.version must equal(ns.version + 1)
      updated.find("/foo/new") match {
        case None =>
          fail("no node /foo/new defined")
        case Some(node) =>
          node.name must equal("new")
          node.data must be === ByteString("test data")
          node.children must equal(Map.empty)
          node.stat.dataVersion must equal(updated.version)
          node.stat.childrenVersion must equal(updated.version)
      }
    }

    "create a new sequential node" in {
      val ctime = DateTime.now()
      val updated = ns.create("/foo/seq", ByteString("test data"), ns.version + 1, ctime, isSequential = true).get
      updated.version must equal(ns.version + 1)
      updated.find("/foo/seq-00000001") match {
        case None =>
          fail("no node /foo/seq-00000001 defined")
        case Some(node) =>
          node.name must equal("seq-00000001")
          node.data must be === ByteString("test data")
          node.children must equal(Map.empty)
          node.stat.dataVersion must equal(updated.version)
          node.stat.childrenVersion must equal(updated.version)
          updated.get("/foo").stat.seqCounter must equal(1)
      }
    }

    "return failure when attempting to create a node whose parent doesn't exist" in {
      val ctime = DateTime.now()
      ns.create("/foo/bar/new", ByteString("test data"), ns.version + 1, ctime, isSequential =  false) match {
        case Success(_) => fail("creating node whose parent doesn't exist should return Failure")
        case Failure(ex: InvalidPathException) => // success
        case Failure(ex) => fail("node creation failed with unexpected exception: {}", ex)
      }
    }

    "return failure when attempting to create a node which already exists" in {
      val ctime = DateTime.now()
      ns.create("/foo", ByteString("test data"), ns.version + 1, ctime, isSequential = false) match {
        case Success(_) => fail("creating node which already exists should return Failure")
        case Failure(ex: InvalidPathException) => // success
        case Failure(ex) => fail("node creation failed with unexpected exception: {}", ex)
      }
    }

    "delete an existing node" in {
      val mtime = DateTime.now()
      val updated = ns.delete("/foo", None, ns.version + 1, mtime).get
      updated.version must equal(ns.version + 1)
      updated.exists("/foo") must equal(false)
    }

    "delete an existing node with a version specified" in {
      val mtime = DateTime.now()
      val updated = ns.delete("/foo", Some(2), ns.version + 1, mtime).get
      updated.version must equal(ns.version + 1)
      updated.exists("/foo") must equal(false)
    }

    "return failure when attempting to delete a node which doesn't exist" in {
      val mtime = DateTime.now()
      ns.delete("/foo/bar/new", None, ns.version + 1, mtime) match {
        case Success(_) => fail("deleting node which doesn't exist should return Failure")
        case Failure(ex: InvalidPathException) => // success
        case Failure(ex) => fail("node deletion failed with unexpected exception: {}", ex)
      }
    }

    "return failure when attempting to delete the root node" in {
      val mtime = DateTime.now()
      ns.delete("/", None, ns.version + 1, mtime) match {
        case Success(_) => fail("deleting the root node should return Failure")
        case Failure(ex: RootModification) => // success
        case Failure(ex) => fail("node deletion failed with unexpected exception: {}", ex)
      }
    }
  }
}
