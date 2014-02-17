package com.syntaxjockey.smr

import akka.actor.ActorRef

/**
 *
 */
case class ConfigurationState(states: Vector[Configuration])

/**
 *
 */

case class Configuration(peers: Set[ActorRef])

