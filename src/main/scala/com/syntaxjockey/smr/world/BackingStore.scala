package com.syntaxjockey.smr.world

import scala.util.{Success, Try}

/**
 *
 */
trait BackingStore {
  /**
   *
   */
  def getWorld: WorldState
  /**
   *
   */
  def updateWorld(world: WorldState): Try[Unit]
}

/**
 *
 */
class NullBackingStore extends BackingStore {
  def getWorld = WorldState.void
  def updateWorld(world: WorldState) = Success(Unit)
}
