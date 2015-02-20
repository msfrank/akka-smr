package com.syntaxjockey.smr

import org.scalatest.{ShouldMatchers, WordSpec}
import akka.util.ByteString
import org.joda.time.DateTime
import scala.util.{Success, Failure}

import com.syntaxjockey.smr.command._
import com.syntaxjockey.smr.world.{EphemeralWorld, PathConversions, World}
import PathConversions._

class TransactionSpec extends WordSpec with ShouldMatchers {

  "A TransactionCommand" should {

    "succeed applying multiple mutations" in {
      val transaction = TransactionCommand(Vector(
        CreateNode("/n1", ByteString(), DateTime.now()),
        SetNodeData("/n1", ByteString("hello, world!"), None, DateTime.now())
      ))
      transaction.apply(new EphemeralWorld) match {
        case Failure(ex) =>
          fail("transaction failed", ex)
        case Success(Response(world, result: TransactionResult, notifications)) =>
          result.results.length should be(2)
          val node = world.getNode("/n1").get
          node.data shouldEqual ByteString("hello, world!")
        case Success(result) =>
          fail("transaction didn't return TransactionResult")
      }
    }

    "fail completely if any single mutation fails" in {

      val transaction = TransactionCommand(Vector(
        CreateNode("/n1", ByteString.empty, DateTime.now()),
        SetNodeData("/n1", ByteString("hello, world!"), None, DateTime.now()),
        SetNodeData("/absent", ByteString.empty, None, DateTime.now())
      ))
      transaction.apply(new EphemeralWorld) match {
        case Failure(ex) =>
          //
        case Success(result) =>
          fail("transaction didn't fail")
      }
    }

    "fail if the transaction is empty" in {
      TransactionCommand(Vector.empty).apply(new EphemeralWorld) match {
        case Failure(ex) =>
          if (!ex.isInstanceOf[IllegalArgumentException])
            fail("empty transaction failed with unexpected exception", ex)
        case Success(_) =>
          fail("empty transaction should not succeed")
      }
    }
  }
}
