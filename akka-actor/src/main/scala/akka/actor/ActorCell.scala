/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.actor

import akka.event.EventHandler
import akka.config.Supervision._
import akka.dispatch._
import akka.util._
import java.util.{ Collection ⇒ JCollection }
import java.util.concurrent.{ ScheduledFuture, ConcurrentHashMap, TimeUnit }
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable.Stack

/**
 * The actor context - the view of the actor cell from the actor.
 * Exposes contextual information for the actor and the current message.
 * TODO: everything here for current compatibility - could be limited more
 */
private[akka] trait ActorContext {

  def self: ActorRef with ScalaActorRef

  def receiveTimeout: Option[Long]

  def receiveTimeout_=(timeout: Option[Long]): Unit

  def hotswap: Stack[PartialFunction[Any, Unit]]

  def hotswap_=(stack: Stack[PartialFunction[Any, Unit]]): Unit

  def currentMessage: MessageInvocation

  def currentMessage_=(invocation: MessageInvocation): Unit

  def sender: Option[ActorRef]

  def senderFuture(): Option[Promise[Any]]

  def channel: UntypedChannel

  def linkedActors: JCollection[ActorRef]

  def dispatcher: MessageDispatcher

  def handleDeath(death: Death)
}

private[akka] object ActorCell {
  sealed trait Status
  object Status {
    object Running extends Status
    object Shutdown extends Status
  }

  val contextStack = new ThreadLocal[Stack[ActorContext]] {
    override def initialValue = Stack[ActorContext]()
  }
}

private[akka] class ActorCell(
  val self: ActorRef with ScalaActorRef,
  props: Props,
  _receiveTimeout: Option[Long],
  _hotswap: Stack[PartialFunction[Any, Unit]])
  extends ActorContext {

  import ActorCell._

  val guard = new ReentrantGuard // TODO: remove this last synchronization point

  @volatile
  var status: Status = Status.Running

  @volatile
  var mailbox: AnyRef = _

  @volatile
  var futureTimeout: Option[ScheduledFuture[AnyRef]] = None

  @volatile
  var _supervisor: Option[ActorRef] = None

  @volatile
  var maxNrOfRetriesCount: Int = 0

  @volatile
  var restartTimeWindowStartNanos: Long = 0L

  @volatile
  lazy val _linkedActors = new ConcurrentHashMap[Uuid, ActorRef]

  @volatile
  var hotswap: Stack[PartialFunction[Any, Unit]] = _hotswap // TODO: currently settable from outside for compatibility

  @volatile
  var receiveTimeout: Option[Long] = _receiveTimeout // TODO: currently settable from outside for compatibility

  @volatile
  var currentMessage: MessageInvocation = null

  val actor: AtomicReference[Actor] = new AtomicReference[Actor]()

  def ref: ActorRef with ScalaActorRef = self

  def uuid: Uuid = self.uuid

  def actorClass: Class[_] = actor.get.getClass

  def dispatcher: MessageDispatcher = props.dispatcher

  def isRunning: Boolean = status == Status.Running
  def isShutdown: Boolean = status == Status.Shutdown

  def start(): Unit = {
    if (isShutdown) throw new ActorStartException("Can't start an actor that has been stopped")
    if (props.supervisor.isDefined) props.supervisor.get.link(self)
    dispatcher.attach(this)
    Actor.registry.register(self)
  }

  def newActor(restart: Boolean): Actor = {
    val stackBefore = contextStack.get
    contextStack.set(stackBefore.push(this))
    try {
      if (restart) {
        val a = actor.get()
        val fresh = try a.freshInstance catch {
          case e ⇒
            EventHandler.error(e, a, "freshInstance() failed, falling back to initial actor factory")
            None
        }
        fresh match {
          case Some(actor) ⇒ actor
          case None        ⇒ props.creator()
        }
      } else {
        props.creator()
      }
    } finally {
      val stackAfter = contextStack.get
      if (stackAfter.nonEmpty)
        contextStack.set(if (stackAfter.head eq null) stackAfter.pop.pop else stackAfter.pop) // pop null marker plus our context
    }
  } match {
    case null  ⇒ throw new ActorInitializationException("Actor instance passed to actorOf can't be 'null'")
    case valid ⇒ valid
  }

  def suspend(): Unit = dispatcher.suspend(this)

  def resume(): Unit = dispatcher.resume(this)

  private[akka] def stop(): Unit = guard.withGuard {
    if (isRunning) {
      receiveTimeout = None
      cancelReceiveTimeout
      Actor.registry.unregister(self)
      status = Status.Shutdown
      dispatcher.detach(this)
      try {
        val a = actor.get
        if (Actor.debugLifecycle) EventHandler.debug(a, "stopping")
        if (a ne null) a.postStop()

        { //Stop supervised actors
          val i = _linkedActors.values.iterator
          while (i.hasNext) {
            i.next.stop()
            i.remove()
          }
        }

      } finally {
        //if (supervisor.isDefined) supervisor.get ! Death(self, new ActorKilledException("Stopped"), false)
        currentMessage = null
        clearActorContext()
      }
    }
  }

  def link(actorRef: ActorRef): ActorRef = {
    guard.withGuard {
      val actorRefSupervisor = actorRef.supervisor
      val hasSupervisorAlready = actorRefSupervisor.isDefined
      if (hasSupervisorAlready && actorRefSupervisor.get.uuid == self.uuid) return actorRef // we already supervise this guy
      else if (hasSupervisorAlready) throw new IllegalActorStateException(
        "Actor can only have one supervisor [" + actorRef + "], e.g. link(actor) fails")
      else {
        _linkedActors.put(actorRef.uuid, actorRef)
        actorRef.supervisor = Some(self)
      }
    }
    if (Actor.debugLifecycle) EventHandler.debug(actor.get(), "now supervising " + actorRef)
    actorRef
  }

  def unlink(actorRef: ActorRef): ActorRef = {
    guard.withGuard {
      if (_linkedActors.remove(actorRef.uuid) eq null)
        throw new IllegalActorStateException("Actor [" + actorRef + "] is not a linked actor, can't unlink")
      actorRef.supervisor = None
      if (Actor.debugLifecycle) EventHandler.debug(actor.get(), "stopped supervising " + actorRef)
    }
    actorRef
  }

  def linkedActors: JCollection[ActorRef] = java.util.Collections.unmodifiableCollection(_linkedActors.values)

  def supervisor: Option[ActorRef] = _supervisor

  def supervisor_=(sup: Option[ActorRef]): Unit = _supervisor = sup

  def postMessageToMailbox(message: Any, channel: UntypedChannel): Unit =
    if (isRunning) dispatcher dispatchMessage new MessageInvocation(this, message, channel)
    else throw new ActorInitializationException("Actor " + self + " is dead")

  def postMessageToMailboxAndCreateFutureResultWithTimeout(
    message: Any,
    timeout: Timeout,
    channel: UntypedChannel): Future[Any] = if (isRunning) {
    val future = channel match {
      case f: ActorPromise ⇒ f
      case _               ⇒ new ActorPromise(timeout)(dispatcher)
    }
    dispatcher dispatchMessage new MessageInvocation(this, message, future)
    future
  } else throw new ActorInitializationException("Actor " + self + " is dead")

  def sender: Option[ActorRef] = {
    val msg = currentMessage
    if (msg eq null) None
    else msg.channel match {
      case ref: ActorRef ⇒ Some(ref)
      case _             ⇒ None
    }
  }

  def senderFuture(): Option[Promise[Any]] = {
    val msg = currentMessage
    if (msg eq null) None
    else msg.channel match {
      case f: ActorPromise ⇒ Some(f)
      case _               ⇒ None
    }
  }

  def channel: UntypedChannel = currentMessage match {
    case null ⇒ NullChannel
    case msg  ⇒ msg.channel
  }

  def invoke(messageHandle: MessageInvocation): Unit = {
    guard.lock.lock()
    try {
      if (!isShutdown) {
        currentMessage = messageHandle
        try {
          try {
            cancelReceiveTimeout() // FIXME: leave this here?

            val a = actor.get() match {
              case null ⇒
                val created = newActor(restart = false)
                actor.set(created)
                if (Actor.debugLifecycle) EventHandler.debug(created, "started")
                created.preStart()
                created
              case instance ⇒ instance
            }

            a.apply(messageHandle.message)
            currentMessage = null // reset current message after successful invocation
          } catch {
            case e ⇒
              EventHandler.error(e, self, e.getMessage)

              // prevent any further messages to be processed until the actor has been restarted
              dispatcher.suspend(this)

              channel.sendException(e)

              if (supervisor.isDefined) supervisor.get ! Death(self, e, true) else dispatcher.resume(this)

              if (e.isInstanceOf[InterruptedException]) throw e //Re-throw InterruptedExceptions as expected
          } finally {
            checkReceiveTimeout // Reschedule receive timeout
          }
        } catch {
          case e ⇒
            EventHandler.error(e, actor.get(), e.getMessage)
            throw e
        }
      } else {
        // throwing away message if actor is shut down, no use throwing an exception in receiving actor's thread, isShutdown is enforced on caller side
      }
    } finally {
      guard.lock.unlock()
    }
  }

  def handleDeath(death: Death): Unit = {
    props.faultHandler match {
      case AllForOnePermanentStrategy(trapExit, maxRetries, within) if trapExit.exists(_.isAssignableFrom(death.cause.getClass)) ⇒
        restartLinkedActors(death.cause, maxRetries, within)

      case AllForOneTemporaryStrategy(trapExit) if trapExit.exists(_.isAssignableFrom(death.cause.getClass)) ⇒
        restartLinkedActors(death.cause, None, None)

      case OneForOnePermanentStrategy(trapExit, maxRetries, within) if trapExit.exists(_.isAssignableFrom(death.cause.getClass)) ⇒
        death.deceased.restart(death.cause, maxRetries, within)

      case OneForOneTemporaryStrategy(trapExit) if trapExit.exists(_.isAssignableFrom(death.cause.getClass)) ⇒
        death.deceased.stop()
        self ! MaximumNumberOfRestartsWithinTimeRangeReached(death.deceased, None, None, death.cause)

      case _ ⇒
        if (_supervisor.isDefined) throw death.cause else death.deceased.stop() //Escalate problem if not handled here
    }
  }

  def restart(reason: Throwable, maxNrOfRetries: Option[Int], withinTimeRange: Option[Int]) {
    def performRestart() {
      val failedActor = actor.get
      if (Actor.debugLifecycle) EventHandler.debug(failedActor, "restarting")
      val message = if (currentMessage ne null) Some(currentMessage.message) else None
      if (failedActor ne null) failedActor.preRestart(reason, message)
      val freshActor = newActor(restart = true)
      clearActorContext()
      actor.set(freshActor) // assign it here so if preStart fails, we can null out the sef-refs next call
      freshActor.postRestart(reason)
      if (Actor.debugLifecycle) EventHandler.debug(freshActor, "restarted")
    }

    @tailrec
    def attemptRestart() {
      val success = if (requestRestartPermission(maxNrOfRetries, withinTimeRange)) {
        guard.withGuard[Boolean] {
          val success =
            try {
              performRestart()
              true
            } catch {
              case e ⇒
                EventHandler.error(e, self, "Exception in restart of Actor [%s]".format(toString))
                false // an error or exception here should trigger a retry
            } finally {
              currentMessage = null
            }

          if (success) {
            dispatcher.resume(this)
            restartLinkedActors(reason, maxNrOfRetries, withinTimeRange)
          }
          success
        }
      } else {
        // tooManyRestarts
        if (supervisor.isDefined)
          supervisor.get ! MaximumNumberOfRestartsWithinTimeRangeReached(self, maxNrOfRetries, withinTimeRange, reason)
        stop()
        true // done
      }

      if (success) () // alles gut
      else attemptRestart()
    }

    attemptRestart() // recur
  }

  def requestRestartPermission(maxNrOfRetries: Option[Int], withinTimeRange: Option[Int]): Boolean = {
    val denied = if (maxNrOfRetries.isEmpty && withinTimeRange.isEmpty) {
      // immortal
      false
    } else if (withinTimeRange.isEmpty) {
      // restrict number of restarts
      val retries = maxNrOfRetriesCount + 1
      maxNrOfRetriesCount = retries //Increment number of retries
      retries > maxNrOfRetries.get
    } else {
      // cannot restart more than N within M timerange
      val retries = maxNrOfRetriesCount + 1

      val windowStart = restartTimeWindowStartNanos
      val now = System.nanoTime
      // we are within the time window if it isn't the first restart, or if the window hasn't closed
      val insideWindow = if (windowStart == 0) true else (now - windowStart) <= TimeUnit.MILLISECONDS.toNanos(withinTimeRange.get)

      if (windowStart == 0 || !insideWindow) //(Re-)set the start of the window
        restartTimeWindowStartNanos = now

      // reset number of restarts if window has expired, otherwise, increment it
      maxNrOfRetriesCount = if (windowStart != 0 && !insideWindow) 1 else retries // increment number of retries

      val restartCountLimit = if (maxNrOfRetries.isDefined) maxNrOfRetries.get else 1

      // the actor is dead if it dies X times within the window of restart
      insideWindow && retries > restartCountLimit
    }

    denied == false // if we weren't denied, we have a go
  }

  protected[akka] def restartLinkedActors(reason: Throwable, maxNrOfRetries: Option[Int], withinTimeRange: Option[Int]): Unit = {
    props.faultHandler.lifeCycle match {
      case Temporary ⇒
        val i = _linkedActors.values.iterator
        while (i.hasNext) {
          val actorRef = i.next()

          i.remove()

          actorRef.stop()

          //FIXME if last temporary actor is gone, then unlink me from supervisor <-- should this exist?
          if (!i.hasNext && supervisor.isDefined)
            supervisor.get ! UnlinkAndStop(self)
        }

      case Permanent ⇒
        val i = _linkedActors.values.iterator
        while (i.hasNext) i.next().restart(reason, maxNrOfRetries, withinTimeRange)
    }
  }

  def checkReceiveTimeout() {
    cancelReceiveTimeout()
    val recvtimeout = receiveTimeout
    if (recvtimeout.isDefined && dispatcher.mailboxIsEmpty(this)) {
      //Only reschedule if desired and there are currently no more messages to be processed
      futureTimeout = Some(Scheduler.scheduleOnce(self, ReceiveTimeout, recvtimeout.get, TimeUnit.MILLISECONDS))
    }
  }

  def cancelReceiveTimeout() {
    if (futureTimeout.isDefined) {
      futureTimeout.get.cancel(true)
      futureTimeout = None
    }
  }

  def clearActorContext(): Unit = setActorContext(null)

  def setActorContext(newContext: ActorContext): Unit = {
    @tailrec
    def lookupAndSetSelfFields(clazz: Class[_], actor: Actor, newContext: ActorContext): Boolean = {
      val success = try {
        val contextField = clazz.getDeclaredField("context")
        contextField.setAccessible(true)
        contextField.set(actor, newContext)
        true
      } catch {
        case e: NoSuchFieldException ⇒ false
      }

      if (success) true
      else {
        val parent = clazz.getSuperclass
        if (parent eq null)
          throw new IllegalActorStateException(toString + " is not an Actor since it have not mixed in the 'Actor' trait")
        lookupAndSetSelfFields(parent, actor, newContext)
      }
    }

    lookupAndSetSelfFields(actor.get.getClass, actor.get, newContext)
  }

  override def hashCode: Int = HashCode.hash(HashCode.SEED, uuid)

  override def equals(that: Any): Boolean = {
    that.isInstanceOf[ActorCell] && that.asInstanceOf[ActorCell].uuid == uuid
  }

  override def toString = "ActorCell[%s]".format(uuid)
}

