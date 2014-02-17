package com.syntaxjockey.smr.raft

import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr._

/**
 *
 */
case class ConfigurationCommand(config: Configuration) extends Command {
  def apply(world: WorldState): Try[WorldStateResult] = {
    val transformed = WorldState(world.version, world.namespaces, ConfigurationState(world.config.states.tail))
    Success(WorldStateResult(transformed, ConfigurationResult))
  }
}

/**
 *
 */
case object ConfigurationResult extends Result
