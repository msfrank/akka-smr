package com.syntaxjockey.smr.world

import akka.actor.ActorRef

/**
 *
 */
case class Configuration(peers: Set[ActorRef])

