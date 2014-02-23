package com.syntaxjockey.smr.world

import com.syntaxjockey.smr.namespace.Namespace
import scala.util.{Success, Try}
import com.syntaxjockey.smr.{Result, Command, ConfigurationState}

/**
 * Contains the state of the entire world.  Every state machine operation takes
 * the current world state as the input, and returns a transformed world state as
 * the output, without any side effects.
 */
case class WorldState(version: Long, namespaces: Map[String,Namespace], config: ConfigurationState)

object WorldState {
  /**
   * creatio ex nihilo, aka 'the singularity' :)
   */
  val void = WorldState(0, Map.empty, ConfigurationState(Vector.empty))
}

