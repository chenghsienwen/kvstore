package kvstore

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import akka.event.LoggingReceive

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.collection.mutable

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  case class Distribute(key: String, valueOption: Option[String], id: Long)
  case class DistributionContext(originator: ActorRef, replicators: Set[ActorRef], persisted: Boolean, schedule: Cancellable, startInMillis: Long)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv =  mutable.Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persistence = context.actorOf(persistenceProps, "persistence")
  val distribution =  mutable.Map.empty[Long, DistributionContext]

  var sequenceNumber: Long = 0
  def nextSequence() {
    sequenceNumber += 1
  }

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => println("get JoinedPrimary, send to handler");context.become(leader)
    case JoinedSecondary => println("get JoinedSecondary, send to handler");context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  private def leader = LoggingReceive(_leader orElse handleDistribution(Some(1000), (key, id) => OperationAck(id)))
  val _leader: Receive = {
    case Insert(key, value, id) => {
      kv += (key->value)
      distribute(key, Some(value), id)
    }
    case Replicas(replicas) => {
      val oldReplicators = (secondaries.keys.toSet -- replicas).map(secondaries(_))
      oldReplicators foreach { _ ! PoisonPill }
      distribution foreach { case (id, DistributionContext(originator, distributionReplicators, persisted, schedule, startInMillis)) =>
        distribution(id) = DistributionContext(originator, distributionReplicators -- oldReplicators, persisted, schedule, startInMillis)
      }

      secondaries = replicas.filterNot(_ == self).map(replica => replica -> context.actorOf(Replicator.props(replica))).toMap
      replicators = secondaries.values.toSet

      kv foreach { case (key, value) =>
        replicators foreach { replicator =>
          replicator ! Replicate(key, Some(value), 0)
        }
      }
    }
    case Remove(key, id) => {
      kv -= key
      distribute(key, None, id)
    }
    case Get(key, id) => {
      kv.contains(key) match {
        case true => sender ! GetResult(key, kv.get(key), id)
        case false => sender ! GetResult(key, None, id)
      }
    }
  }

  /* TODO Behavior for the replica role. */
  private def replica = LoggingReceive(_replica orElse handleDistribution(None, SnapshotAck))
  val _replica: Receive = {
    case Get(key, id) => {
      println(s"request $key, $id")
      kv.contains(key) match {
        case true => sender ! GetResult(key, kv.get(key), id)
        case false => sender ! GetResult(key, None, id)
      }
    }
    case Snapshot(key, valueOpt, seq) => {
      println(s"get snapshot from $key, $valueOpt, id: $seq, current seq: $sequenceNumber")
      seq == sequenceNumber match {
        case true => {
          valueOpt match {
            case Some(v) => kv += (key->v)
            case None => kv -= key
          }
          distribute(key, valueOpt, seq)
        }
        case false  => seq < sequenceNumber match {
          case true =>sender ! SnapshotAck(key, seq)
          case false =>
        }
      }
    }
  }

  private def handleDistribution(timeoutInMillis: Option[Long], acknowledgement: (String, Long) => Any): Receive = {
    case message @ Distribute(key, valueOption, id) =>
      if (distribution.contains(id)) {
        val DistributionContext(originator, replicators, persisted, schedule, startInMillis) = distribution(id)
        if (replicators.isEmpty && persisted) {
          schedule.cancel()
          originator ! acknowledgement(key, id)
          nextSequence()
          distribution -= id
        } else if (timeoutInMillis.isDefined && now > startInMillis + timeoutInMillis.get) {
          originator ! OperationFailed(id)
          nextSequence()
          distribution -= id
        } else if (!persisted) {
          persistence ! Persist(key, valueOption, id)
        }
      }

    case Persisted(key, id) =>
      println(s"get Persisted $key id: $id")
      if (distribution.contains(id)) {
        val DistributionContext(originator, replicators, _, schedule, startInMillis) = distribution(id)
        distribution(id) = DistributionContext(originator, replicators, persisted = true, schedule, startInMillis)
      }

    case Replicated(key, id) =>
      if (distribution.contains(id)) {
        val DistributionContext(originator, replicators, persisted, schedule, startInMillis) = distribution(id)
        distribution(id) = DistributionContext(originator, replicators - sender, persisted, schedule, startInMillis)
      }
  }

  private def distribute(key: String, valueOption: Option[String], id: Long) {
    replicators foreach { replicator =>
      replicator ! Replicate(key, valueOption, id)
    }

    val schedule = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, self, Distribute(key, valueOption, id))
    distribution(id) = DistributionContext(sender, replicators, persisted = false, schedule, now)
    println(s"distribute key:$key id:$id ")
  }

  private def now = System.currentTimeMillis()

}

