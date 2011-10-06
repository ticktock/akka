package akka.cluster.coordination.postgres

import akka.cluster.coordination._
import akka.actor.ActorRef

class PostgresCoordinationLock(path: String, listener: CoordinationLockListener, coordinationActor: ActorRef) extends CoordinationLock {
  def getId: String = null

  def isOwner: Boolean = false

  def lock(): Boolean = false

  def unlock() = null
}