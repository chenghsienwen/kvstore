/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore.given

import akka.actor.{ Props, Actor }
import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger

object Persistence {
  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {
  import kvstore.Persistence._

  private def newFailCount: Int = if (flaky) Random.nextInt(4) else 0
  var failSteps: Int = newFailCount

  def receive = {
    case Persist(key, value, id) =>
      println(s" get Persisit $key, value $value, id: $id")
      if (failSteps == 0) {
        sender ! Persisted(key, id)
        failSteps = newFailCount
      } else failSteps -= 1
  }

}
