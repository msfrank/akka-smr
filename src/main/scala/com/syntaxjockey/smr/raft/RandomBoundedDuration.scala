package com.syntaxjockey.smr.raft

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

case class RandomBoundedDuration(lowerBound: FiniteDuration, upperBound: FiniteDuration, resolution: TimeUnit = TimeUnit.MILLISECONDS) {
  if (lowerBound > upperBound)
    throw new IllegalArgumentException("lowerBound is greater than upperBound")
  val range = upperBound - lowerBound

  def nextDuration: FiniteDuration = {
    import scala.concurrent.duration._
    val doubleDuration = lowerBound.toUnit(resolution) + (range.toUnit(resolution) * Random.nextDouble())
    FiniteDuration(doubleDuration.toLong, resolution)
  }
}

object RandomBoundedDuration {
  import scala.language.implicitConversions
  implicit def randomBounded2FiniteDuration(r: RandomBoundedDuration): FiniteDuration = r.nextDuration
}
