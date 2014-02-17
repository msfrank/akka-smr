package com.syntaxjockey.smr

import akka.actor._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

import com.syntaxjockey.smr.raft.RandomBoundedDuration

trait SMREvent
case object SMRClusterReadyEvent extends SMREvent
case object SMRClusterLostEvent extends SMREvent
case object SMRClusterChangedEvent extends SMREvent

/**
 * Forwards SMREvent messages from the SMRExtension to the ActorSystem event stream.
 */
class SMREventStream extends Actor {
  def receive = {
    case event: SMREvent => context.system.eventStream.publish(event)
  }
}

/**
 * SMRExtension implementation.
 */
class SMRExtensionImpl(system: ActorSystem) extends Extension {
  val config = system.settings.config.getConfig("akka.smr")
  val smrName = config.getString("smr-name")
  val minimumNrProcessors = config.getInt("minimum-nr-processors")
  val electionTimeout = config.getDuration("election-timeout", TimeUnit.MILLISECONDS)
  val electionTimeoutVariance = config.getDuration("election-timeout-variance", TimeUnit.MILLISECONDS)
  val idleTimeout = config.getDuration("idle-timeout", TimeUnit.MILLISECONDS)
  val maxEntriesBatch = config.getInt("max-entries-batch")
  val eventStream = system.actorOf(Props(classOf[SMREventStream]))
  val rsm = {
    val lowerBound = FiniteDuration(electionTimeout, TimeUnit.MILLISECONDS)
    val upperBound = FiniteDuration(electionTimeout + electionTimeoutVariance, TimeUnit.MILLISECONDS)
    val _electionTimeout = RandomBoundedDuration(lowerBound, upperBound)
    val _idleTimeout = FiniteDuration(idleTimeout, TimeUnit.MILLISECONDS)
    system.actorOf(ReplicatedStateMachine.props(eventStream, minimumNrProcessors, _electionTimeout, _idleTimeout, maxEntriesBatch), smrName)
  }
}

/**
 * Entry point to the SMR layer.
 */
object SMRExtension extends ExtensionId[SMRExtensionImpl] with ExtensionIdProvider {
  override def lookup() = SMRExtension
  override def createExtension(system: ExtendedActorSystem) = new SMRExtensionImpl(system)
  override def get(system: ActorSystem): SMRExtensionImpl = super.get(system)
}

/**
 * Shorthand entry point to the SMR layer.
 */
object SMR {
  def apply(system: ActorSystem): ActorRef = SMRExtension(system).rsm
}