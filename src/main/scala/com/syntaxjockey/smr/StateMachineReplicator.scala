package com.syntaxjockey.smr

import akka.actor.ActorRef

object StateMachineReplicator {

  case class Request(command: Command, caller: ActorRef)

  case class Submit(command: Command, clientId: String, lsn: Long)
  case class Accept(clientId: String, lsn: Long)
  case class Execute(clientId: String, lsn: Long)
  case class Return(result: Result, clientId: String, lsn: Long)
}

/**
 *  marker trait for a command operation.
 */
trait Command

/**
 * marker trait for an operation result.
 */
trait Result
