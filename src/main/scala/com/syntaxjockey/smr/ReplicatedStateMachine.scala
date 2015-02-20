package com.syntaxjockey.smr

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.syntaxjockey.smr.command.Command
import com.syntaxjockey.smr.raft.RaftProcessor.Incubating
import com.syntaxjockey.smr.world.{Configuration, Path}
import scala.concurrent.duration._
import scala.collection.SortedSet

import com.syntaxjockey.smr.raft._

/**
 * Proxy actor for coordinating RaftProcessors in a cluster.
 */
class ReplicatedStateMachine(monitor: ActorRef,
                             settings: RaftProcessorSettings,
                             processorRole: Option[String])
extends Actor with ActorLogging {
  import ReplicatedStateMachine._
  import com.syntaxjockey.smr.raft.RaftProcessor.Leader
  import context.dispatcher

  // config
  val localProcessor = context.actorOf(RaftProcessor.props(self, settings))

  // state
  var clusterState: CurrentClusterState = CurrentClusterState(SortedSet.empty, Set.empty, Set.empty, None, Map.empty)
  var remoteProcessors: Map[Address,ActorRef] = Map.empty
  var leader: Option[ActorRef] = None
  var inflight: Option[Request] = None
  var buffered: Vector[Request] = Vector.empty
  var accepted: Vector[Request] = Vector.empty
  var watches: Map[Path,Set[ActorRef]] = Map.empty

  // subscribe to cluster membership events
  Cluster(context.system).subscribe(self, InitialStateAsEvents, classOf[MemberEvent])

  // read current state every 5 minutes
  val readClusterState = context.system.scheduler.schedule(5.minutes, 5.minutes, self, ReadCurrentClusterState)

  def receive = {

    /*
     * Processor discovery/configuration protocol:
     *  1. We subscribe to cluster MemberEvent messages.
     *  2. When a MemberUp message is received, if the member is not this actor system, we have
     *     not seen the member before, and optionally if the member has the appropriate cluster
     *     role, then we send the IdentifyProcessor message to the remote actor system.  we use
     *     the same actor path as this actor.
     *  3. On the remote side, if there is a ReplicatedStateMachine actor at the specified path,
     *     then it replies with a ProcessorIdentity message containing the actor ref for the
     *     RaftProcessor actor.
     *  4. If the remote actor system does not have a ReplicatedStateMachine listening, or there
     *     is some other network error causing the message to be lost, then we never receive a
     *     ProcessorIdentity message back.  To handle this case, we read the entire cluster state
     *     every 5 minutes, and perform the same steps for any member found.  These retries happen
     *     indefinitely.
     */
    case ReadCurrentClusterState =>
      clusterState = Cluster(context.system).state
      clusterState.members.filter { member =>
        !remoteProcessors.contains(member.address) && member.address != self.path.address
      }.foreach { member =>
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! Identify(member)
      }

    case MemberUp(member) =>
      if (!remoteProcessors.contains(member.address) &&
        member.address != Cluster(context.system).selfAddress &&
        (processorRole.isEmpty || member.hasRole(processorRole.get))) {
        val selection = context.actorSelection(self.path.toStringWithAddress(member.address))
        selection ! IdentifyProcessor
        log.debug("member {} is up", member)
      }

    case IdentifyProcessor =>
      sender() ! ProcessorIdentity(localProcessor)

    case ProcessorIdentity(ref) =>
      log.debug("found remote processor {}", ref.path)
      remoteProcessors = remoteProcessors + (ref.path.address -> ref)
      localProcessor ! Configuration(remoteProcessors.values.toSet + localProcessor)

    case MemberExited(member) =>
      log.info("member {} has exited", member)

    case UnreachableMember(member) =>
      log.warning("member {} detected as unreachable", member)

    case MemberRemoved(member, previousStatus) =>
      if (member.address == Cluster(context.system).selfAddress) {
        localProcessor ! PoisonPill
        remoteProcessors = Map.empty
        leader = None
        // FIXME: send replies to any actors waiting for a response
        inflight = None
        buffered = Vector.empty
        accepted = Vector.empty
        watches = Map.empty
        readClusterState.cancel()
        // FIXME: move to a shutdown state, reject commands
        // FIXME: convert into a client, so we can still process commands?
      } else {
        remoteProcessors = remoteProcessors - member.address
        localProcessor ! Configuration(remoteProcessors.values.toSet + localProcessor)
        log.info("member {} was removed", member)
      }

     /* return RSM status */
    case RSMStatusQuery =>
      sender ! RSMStatusResult(buffered.length, remoteProcessors.size + 1)

    /*
     * monitor events from the raft processor
     */
    case ProcessorTransitionEvent(prevState, newState) =>
      if (prevState == Leader && newState != Leader)
      log.debug("processor transitions from {} to {}", prevState, newState)
      if (prevState == Incubating && newState != Incubating)
        monitor ! SMRClusterReadyEvent
      if (prevState != Incubating && newState == Incubating)
        monitor ! SMRClusterLostEvent

    case LeaderElectionEvent(newLeader, term) =>
      // FIXME: this logic seems outdated, need to revisit
      leader = if (newLeader == localProcessor) Some(self) else Some(remoteProcessors(newLeader.path.address))
      if (buffered.nonEmpty && inflight.isEmpty) {
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
    case watch @ Watch(command, observer) =>
      watches = watch.updateWatches(watches)
      self.tell(command, sender())

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

    case rejected @ CommandRejected(command) =>
      inflight match {
        case Some(request) =>
          accepted = accepted :+ request
          inflight = None
          log.debug("COMMAND {} was rejected", command)
          request.caller ! rejected
        case None =>
          log.error("COMMAND {} was rejected but is not currently in-flight", command)
      }
      buffered.headOption match {
        case Some(request) =>
          localProcessor ! request.command
          inflight = Some(request)
          buffered = buffered.tail
          log.debug("COMMAND {} submitted", request.command)
        case None => // do nothing
      }

    case CommandAccepted(logEntry) =>
      inflight match {
        case Some(request) =>
          accepted = accepted :+ request
          inflight = None
          log.debug("COMMAND {} was accepted", logEntry.command)
        case None =>
          log.error("COMMAND {} was accepted but is not currently in-flight", logEntry.command)
      }
      buffered.headOption match {
        case Some(request) =>
          localProcessor ! request.command
          inflight = Some(request)
          buffered = buffered.tail
          log.debug("COMMAND {} submitted", request.command)
        case None => // do nothing
      }

    case CommandExecuted(logEntry, result) =>
      log.debug("COMMAND {} returned {}", logEntry.command, result)
      val request = accepted.head
      accepted = accepted.tail
      request.caller ! result

    case NotificationMap(notifications) =>
      notifications.values.foreach { notification =>
        watches = watches.get(notification.path) match {
          case Some(observers) =>
            observers.foreach(_ ! notification)
            watches - notification.path
          case None => watches
        }
      }

//    /* forward internal messages to the processor */
//    case message: RaftProcessorMessage =>
//      localProcessor.forward(message)
//      //log.debug("forwarding message {} from {} to {}", message, sender().path, localProcessor)
  }
}

object ReplicatedStateMachine {
  def props(monitor: ActorRef,
            settings: RaftProcessorSettings,
            processorRole: Option[String]) = {
    Props(classOf[ReplicatedStateMachine], monitor, settings, processorRole)
  }

  case object ReadCurrentClusterState
  case object IdentifyProcessor
  case class ProcessorIdentity(ref: ActorRef)
  case class Request(command: Command, caller: ActorRef)
}

/**
 * Get status of the RSM.
 */
case object RSMStatusQuery
case class RSMStatusResult(commandsQueued: Int, numProcessors: Int)
