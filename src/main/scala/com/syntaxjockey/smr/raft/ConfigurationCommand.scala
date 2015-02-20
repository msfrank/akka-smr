package com.syntaxjockey.smr.raft


import scala.util.{Failure, Success, Try}

import com.syntaxjockey.smr.command.{Response, Result, Command}
import com.syntaxjockey.smr.world.{Configuration, World}

/**
 *
 */
case class ConfigurationCommand(config: Configuration) extends Command {
  def apply(world: World): Try[Response] = {
    world.dropConfiguration()
    Success(Response(world, ConfigurationResult))
  }
}

/**
 *
 */
case object ConfigurationResult extends Result
