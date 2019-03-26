package kvstore

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  var stopRetry = false
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, value, seq) => {
      println(s"get replicate from $key, value: $value, id:$seq")

      val timer = context.system.scheduler.schedule(0.millisecond, 300.microseconds, replica, Snapshot(key, value, seq))
      val v = (sender, Replicate(key, value, seq), timer)
      acks += (nextSeq()->v)
    }
    case SnapshotAck(key, id) => {
      acks.filter{i => val replicate = i._2._2
        replicate.key == key && replicate.id == id
      }.map{ i => val originalSender = i._2._1
        originalSender ! Replicated(key, id)
        i._2._3.cancel()
      }
    }
  }

}
