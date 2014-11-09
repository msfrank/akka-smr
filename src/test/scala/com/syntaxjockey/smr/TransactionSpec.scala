package com.syntaxjockey.smr

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.util.ByteString
import org.joda.time.DateTime
import scala.util.{Success, Failure}

import com.syntaxjockey.smr.namespace._
import com.syntaxjockey.smr.namespace.PathConversions._
import com.syntaxjockey.smr.world.WorldState

class TransactionSpec extends WordSpec with MustMatchers {

  "A TransactionCommand" must {

    "succeed applying multiple mutations" in {
      val transaction = TransactionCommand(Vector(
        CreateNamespace("foo"),
        CreateNode("foo", "/n1", ByteString(), DateTime.now()),
        SetNodeData("foo", "/n1", ByteString("hello, world!"), None, DateTime.now())
      ))
      transaction.apply(WorldState.void) match {
        case Failure(ex) =>
          fail("transaction failed", ex)
        case Success(Response(world, result: TransactionResult, notifications)) =>
          result.results.length must be(3)
          world.namespaces.contains("foo") must be(true)
          val node = world.namespaces("foo").get("/n1")
          node.data must be === ByteString("hello, world!")
        case Success(result) =>
          fail("transaction didn't return TransactionResult")
      }
    }

    "fail completely if any single mutation fails" in {

      val transaction = TransactionCommand(Vector(
        CreateNamespace("foo"),
        CreateNode("foo", "/n1", ByteString.empty, DateTime.now()),
        SetNodeData("foo", "/n1", ByteString("hello, world!"), None, DateTime.now()),
        SetNodeData("foo", "/absent", ByteString.empty, None, DateTime.now()),
        DeleteNamespace("foo")
      ))
      transaction.apply(WorldState.void) match {
        case Failure(ex) =>
          //
        case Success(result) =>
          fail("transaction didn't fail")
      }
    }

    "fail if the transaction is empty" in {
      TransactionCommand(Vector.empty).apply(WorldState.void) match {
        case Failure(ex) =>
          if (!ex.isInstanceOf[IllegalArgumentException])
            fail("empty transaction failed with unexpected exception", ex)
        case Success(_) =>
          fail("empty transaction must not succeed")
      }
    }
  }
}
