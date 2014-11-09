package com.syntaxjockey.smr.raft

import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr._
import com.syntaxjockey.smr.world.WorldState

/**
 *
 */
case class ConfigurationCommand(config: Configuration) extends Command {
  def apply(world: WorldState): Try[Response] = {
    val transformed = WorldState(world.version, world.namespaces, ConfigurationState(world.config.states.tail))
    Success(Response(transformed, ConfigurationResult))
  }
}

/**
 *
 */
case object ConfigurationResult extends Result
