/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.actor

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

import akka.testkit._
import akka.util.duration._
import akka.testkit.Testing.sleepFor
import akka.config.Supervision.{ OneForOnePermanentStrategy }
import akka.dispatch.Future
import java.util.concurrent.{ TimeUnit, CountDownLatch }
import java.lang.IllegalStateException
import akka.util.ReflectiveAccess
import akka.actor.Actor.actorOf

object ActorRefSpec {

  val latch = TestLatch(4)

  class ReplyActor extends Actor {
    var replyTo: Channel[Any] = null

    def receive = {
      case "complexRequest" ⇒ {
        replyTo = channel
        val worker = actorOf(Props[WorkerActor])
        worker ! "work"
      }
      case "complexRequest2" ⇒
        val worker = actorOf(Props[WorkerActor])
        worker ! channel
      case "workDone"      ⇒ replyTo ! "complexReply"
      case "simpleRequest" ⇒ reply("simpleReply")
    }
  }

  class WorkerActor() extends Actor {
    def receive = {
      case "work" ⇒ {
        work
        reply("workDone")
        self.stop()
      }
      case replyTo: Channel[Any] ⇒ {
        work
        replyTo ! "complexReply"
      }
    }

    private def work {
      sleepFor(1 second)
    }
  }

  class SenderActor(replyActor: ActorRef) extends Actor {

    def receive = {
      case "complex"  ⇒ replyActor ! "complexRequest"
      case "complex2" ⇒ replyActor ! "complexRequest2"
      case "simple"   ⇒ replyActor ! "simpleRequest"
      case "complexReply" ⇒ {
        latch.countDown()
      }
      case "simpleReply" ⇒ {
        latch.countDown()
      }
    }
  }

  class OuterActor(val inner: ActorRef) extends Actor {
    def receive = {
      case "self" ⇒ reply(self)
      case x      ⇒ inner forward x
    }
  }

  class FailingOuterActor(val inner: ActorRef) extends Actor {
    val fail = new InnerActor

    def receive = {
      case "self" ⇒ reply(self)
      case x      ⇒ inner forward x
    }
  }

  class FailingInheritingOuterActor(_inner: ActorRef) extends OuterActor(_inner) {
    val fail = new InnerActor
  }

  class InnerActor extends Actor {
    def receive = {
      case "innerself" ⇒ reply(self)
      case other       ⇒ reply(other)
    }
  }

  class FailingInnerActor extends Actor {
    val fail = new InnerActor

    def receive = {
      case "innerself" ⇒ reply(self)
      case other       ⇒ reply(other)
    }
  }

  class FailingInheritingInnerActor extends InnerActor {
    val fail = new InnerActor
  }
}

class ActorRefSpec extends WordSpec with MustMatchers {
  import akka.actor.ActorRefSpec._

  "An ActorRef" must {

    "not allow Actors to be created outside of an actorOf" in {
      intercept[akka.actor.ActorInitializationException] {
        new Actor { def receive = { case _ ⇒ } }
      }

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new Actor {
          val nested = new Actor { def receive = { case _ ⇒ } }
          def receive = { case _ ⇒ }
        })
      }

      def contextStackMustBeEmpty = ActorCell.contextStack.get.headOption must be === None

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new FailingOuterActor(actorOf(new InnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new OuterActor(actorOf(new FailingInnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new FailingInheritingOuterActor(actorOf(new InnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new FailingOuterActor(actorOf(new FailingInheritingInnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new FailingInheritingOuterActor(actorOf(new FailingInheritingInnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new FailingInheritingOuterActor(actorOf(new FailingInnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new OuterActor(actorOf(new InnerActor {
          val a = new InnerActor
        })))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new FailingOuterActor(actorOf(new FailingInheritingInnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new OuterActor(actorOf(new FailingInheritingInnerActor)))
      }

      contextStackMustBeEmpty

      intercept[akka.actor.ActorInitializationException] {
        actorOf(new OuterActor(actorOf({ new InnerActor; new InnerActor })))
      }

      contextStackMustBeEmpty

      (intercept[java.lang.IllegalStateException] {
        actorOf(new OuterActor(actorOf({ throw new IllegalStateException("Ur state be b0rked"); new InnerActor })))
      }).getMessage must be === "Ur state be b0rked"

      contextStackMustBeEmpty
    }

    "be serializable using Java Serialization on local node" in {
      val a = actorOf[InnerActor]

      import java.io._

      val baos = new ByteArrayOutputStream(8192 * 32)
      val out = new ObjectOutputStream(baos)

      out.writeObject(a)

      out.flush
      out.close

      val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
      val readA = in.readObject

      a.isInstanceOf[LocalActorRef] must be === true
      readA.isInstanceOf[LocalActorRef] must be === true
      (readA eq a) must be === true
    }

    "must throw exception on deserialize if not present in local registry and remoting is not enabled" in {
      ReflectiveAccess.RemoteModule.isEnabled must be === false

      val a = actorOf[InnerActor]

      val inetAddress = ReflectiveAccess.RemoteModule.configDefaultAddress

      val expectedSerializedRepresentation = SerializedActorRef(
        a.uuid,
        a.address,
        inetAddress.getAddress.getHostAddress,
        inetAddress.getPort,
        a.timeout)

      Actor.registry.unregister(a)

      import java.io._

      val baos = new ByteArrayOutputStream(8192 * 32)
      val out = new ObjectOutputStream(baos)

      out.writeObject(a)

      out.flush
      out.close

      val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
      (intercept[java.lang.IllegalStateException] {
        in.readObject
      }).getMessage must be === "Trying to deserialize ActorRef [" + expectedSerializedRepresentation + "] but it's not found in the local registry and remoting is not enabled."
    }

    "support nested actorOfs" in {
      val a = actorOf(new Actor {
        val nested = actorOf(new Actor { def receive = { case _ ⇒ } })
        def receive = { case _ ⇒ reply(nested) }
      })

      val nested = (a ? "any").as[ActorRef].get
      a must not be null
      nested must not be null
      (a ne nested) must be === true
    }

    "support advanced nested actorOfs" in {
      val a = actorOf(Props(new OuterActor(actorOf(Props(new InnerActor)))))
      val inner = (a ? "innerself").as[Any].get

      (a ? a).as[ActorRef].get must be(a)
      (a ? "self").as[ActorRef].get must be(a)
      inner must not be a

      (a ? "msg").as[String] must be === Some("msg")
    }

    "support reply via channel" in {
      val serverRef = actorOf(Props[ReplyActor])
      val clientRef = actorOf(Props(new SenderActor(serverRef)))

      clientRef ! "complex"
      clientRef ! "simple"
      clientRef ! "simple"
      clientRef ! "simple"

      latch.await

      latch.reset

      clientRef ! "complex2"
      clientRef ! "simple"
      clientRef ! "simple"
      clientRef ! "simple"

      latch.await

      clientRef.stop()
      serverRef.stop()
    }

    "stop when sent a poison pill" in {
      val timeout = Timeout(20000)
      val ref = actorOf(Props(new Actor {
        def receive = {
          case 5    ⇒ tryReply("five")
          case null ⇒ tryReply("null")
        }
      }))

      val ffive = (ref ? (5, timeout)).mapTo[String]
      val fnull = (ref ? (null, timeout)).mapTo[String]

      intercept[ActorKilledException] {
        (ref ? PoisonPill).get
        fail("shouldn't get here")
      }

      ffive.resultOrException.get must be("five")
      fnull.resultOrException.get must be("null")

      ref.isRunning must be(false)
      ref.isShutdown must be(true)
    }

    "restart when Kill:ed" in {
      filterException[ActorKilledException] {
        val latch = new CountDownLatch(2)

        val boss = actorOf(Props(new Actor {

          val ref = actorOf(
            Props(new Actor {
              def receive = { case _ ⇒ }
              override def preRestart(reason: Throwable, msg: Option[Any]) = latch.countDown()
              override def postRestart(reason: Throwable) = latch.countDown()
            }).withSupervisor(self))

          protected def receive = { case "sendKill" ⇒ ref ! Kill }
        }).withFaultHandler(OneForOnePermanentStrategy(List(classOf[Throwable]), 2, 1000)))

        boss ! "sendKill"
        latch.await(5, TimeUnit.SECONDS) must be === true
      }
    }
  }
}
