package com.syntaxjockey.smr

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent._
import scala.concurrent.duration._
import scala.collection.SortedSet
import scala.util.{Random, Failure, Success}
import java.util.concurrent.TimeUnit

import com.syntaxjockey.smr.raft._

/**
 * Proxy actor for coordinating RaftProcessors in a cluster.
 */
class ReplicatedStateMachine(monitor: ActorRef, minimumProcessors: Int) extends Actor with ActorLogging {
  import ReplicatedStateMachine._
  import com.syntaxjockey.smr.raft.RaftProcessor.Leader
  import context.dispatcher

  // config
  val electionTimeout = FiniteDuration(4500 + Random.nextInt(500), TimeUnit.MILLISECONDS)
  val idleTimeout = 2000.milliseconds
  val localProcessor = context.actorOf(RaftProcessor.props(self, self, electionTimeout, idleTimeout))

  // state
  var clusterState: CurrentClusterState = CurrentClusterState(SortedSet.empty, Set.empty, Set.empty, None, Map.empty)
  var remoteProcessors: Map[Address,ActorRef] = Map.empty
  var leader: Option[ActorRef] = None
  var inflight: Option[Request] = None
  var buffered: Vector[Request] = Vector.empty
  var accepted: Vector[Request] = Vector.empty

  // subscribe to cluster membership events
  Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[MemberEvent])

  // read current state every 5 minutes
  context.system.scheduler.schedule(5 minutes, 5 minutes, self, ReadCurrentClusterState)

  def receive = {

    case ReadCurrentClusterState =>
      clusterState = Cluster(context.system).state
      clusterState.members.filter { member =>
        !remoteProcessors.contains(member.address) && member.address != self.path.address
      }.foreach { member =>
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case MemberUp(member) =>
      if (!remoteProcessors.contains(member.address) && member.address != self.path.address) {
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case ActorIdentity(member: Member, Some(ref)) =>
      if (ref != self) {
        remoteProcessors = remoteProcessors + (member.address -> ref)
        if (remoteProcessors.size >= minimumProcessors - 1) {
          localProcessor ! StartProcessing(remoteProcessors.values.toSet)
          log.debug("replicated state machine is now ready")
        }
      }

    case ActorIdentity(member: Member, None) =>
      log.warning("remote processor not found on member {}", member)

    case UnreachableMember(member) =>
      log.warning("member {} detected as unreachable", member)

    case MemberRemoved(member, previousStatus) =>
      log.warning("member {} has been removed (previous status was {})", member, previousStatus)

     /* return RSM status */
    case RSMStatusQuery =>
      sender ! RSMStatusResult(buffered.length, remoteProcessors.size + 1)

    /*
     * monitor events from the raft processor
     */
    case ProcessorTransitionEvent(prevState, newState) =>
      if (prevState == Leader && newState != Leader)
      log.debug("processor transitions from {} to {}", prevState, newState)

    case LeaderElectionEvent(newLeader, term) =>
      val initialized = leader.isDefined
      leader = if (newLeader == localProcessor) Some(self) else Some(remoteProcessors(newLeader.path.address))
      if (!initialized)
        monitor ! RSMReady
      if (!buffered.isEmpty && inflight.isEmpty) {
        val request = buffered.head
        leader.get ! request.command
        inflight = Some(request)
        buffered = buffered.tail
      }
      log.debug("processor {} is now leader for term {}", newLeader.path, term)

    /*
     * Command protocol:
     *  1. Command is received by the RSM.  if a leader is currently defined, and there are no
     *     other commands buffered, then send the command to the Processor immediately and mark
     *     the command as inflight.  Otherwise, append the command to the end of the buffer.
     *  2. Once the command has been acknowledged by the processor, the processor will reply with
     *     CommandAccepted.
     */
    case command: Command =>
      val request = Request(command, sender())
      if (inflight.isEmpty) {
          localProcessor ! command
          inflight = Some(request)
          log.debug("COMMAND {} submitted", command)
      } else {
          buffered = buffered :+ request
          log.debug("COMMAND {} buffered", command)
      }

    case RetryCommand(command: Command) =>
      log.error("command {} must be retried", command)

    case CommandAccepted(logEntry) =>
      inflight match {
        case Some(request) =>
          accepted = accepted :+ request
          inflight = None
          log.debug("COMMAND {} was accepted")
        case None =>
          log.error("COMMAND {} was accepted but is not currently in-flight", logEntry)
      }
      buffered.headOption match {
        case Some(request) =>
          localProcessor ! request.command
          inflight = Some(request)
          buffered = buffered.tail
          log.debug("COMMAND {} submitted", request.command)
        case None => // do nothing
      }

    case CommandApplied(logEntry, result) =>
      val request = accepted.head
      accepted = accepted.tail
      log.debug("notifying {} that {} returned {}", request.caller.path, logEntry.command, result)
      request.caller ! result

    /* forward internal messages to the processor */
    case message: RaftProcessorMessage =>
      //log.debug("forwarding message {} from {} to {}", message, sender().path, localProcessor)
      localProcessor.forward(message)
  }
}

object ReplicatedStateMachine {
  def props(monitor: ActorRef, minimumProcessors: Int) = Props(classOf[ReplicatedStateMachine], monitor, minimumProcessors)

  case class Request(command: Command, caller: ActorRef)
  case object ReadCurrentClusterState
}

/**
 * Notify the RSM monitor that the cluster has successfully initialized and
 * is ready to accept commands.
 */
case object RSMReady

/**
 * Get status of the RSM.
 */
case object RSMStatusQuery
case class RSMStatusResult(commandsQueued: Int, numProcessors: Int)
