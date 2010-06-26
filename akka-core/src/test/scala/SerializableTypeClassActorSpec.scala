package se.scalablesolutions.akka.actor

import Actor._

import org.scalatest.Spec
import org.scalatest.Assertions
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.google.protobuf.Message

@RunWith(classOf[JUnitRunner])
class SerializableTypeClassActorSpec extends
  Spec with
  ShouldMatchers with
  BeforeAndAfterAll {

  import se.scalablesolutions.akka.serialization.Serializer

  object BinaryFormatMyActor {
    implicit object MyActorFormat extends Format[MyActor] {
      def fromBinary(bytes: Array[Byte], act: MyActor): Unit = {
        val p = Serializer.Protobuf.fromBinary(bytes, Some(classOf[ProtobufProtocol.Counter])).asInstanceOf[ProtobufProtocol.Counter]
        act.count = p.getCount
      }
      def toBinary(ac: MyActor) = 
        ProtobufProtocol.Counter.newBuilder.setCount(ac.count).build.toByteArray
    }
  }

  object BinaryFormatMyActorWithDualCounter {
    implicit object MyActorWithDualCounterFormat extends Format[MyActorWithDualCounter] {
      def fromBinary(bytes: Array[Byte], act: MyActorWithDualCounter): Unit = {
        val p = Serializer.Protobuf.fromBinary(bytes, Some(classOf[ProtobufProtocol.DualCounter])).asInstanceOf[ProtobufProtocol.DualCounter]
        act.count1 = p.getCount1
        act.count2 = p.getCount2
      }
      def toBinary(ac: MyActorWithDualCounter) = 
        ProtobufProtocol.DualCounter.newBuilder.setCount1(ac.count1).setCount2(ac.count2).build.toByteArray
    }
  }

  object BinaryFormatMyStatelessActor {
    implicit object MyStatelessActorFormat extends Format[MyStatelessActor] {
      def fromBinary(bytes: Array[Byte], act: MyStatelessActor): Unit = {
      }
      def toBinary(ac: MyStatelessActor) = Array.empty[Byte]
    }
  }

  describe("Serializable actor") {
    it("should be able to serialize and de-serialize a stateful actor") {
      import BinaryFormatMyActor._
      import ActorSerialization._

      val actor1 = actorOf[MyActor].start
      (actor1 !! "hello").getOrElse("_") should equal("world 1")
      (actor1 !! "hello").getOrElse("_") should equal("world 2")

      val bytes = toBinary(actor1)
      val actor2 = fromBinary(bytes)
      actor2.start
      (actor2 !! "hello").getOrElse("_") should equal("world 3")
    }

    it("should be able to serialize and de-serialize a stateful actor with compound state") {
      import BinaryFormatMyActorWithDualCounter._
      import ActorSerialization._

      val actor1 = actorOf[MyActorWithDualCounter].start
      (actor1 !! "hello").getOrElse("_") should equal("world 1 1")
      (actor1 !! "hello").getOrElse("_") should equal("world 2 2")

      val bytes = toBinary(actor1)
      val actor2 = fromBinary(bytes)
      actor2.start
      (actor2 !! "hello").getOrElse("_") should equal("world 3 3")
    }

    it("should be able to serialize and de-serialize a stateless actor") {
      import BinaryFormatMyStatelessActor._
      import ActorSerialization._

      val actor1 = actorOf[MyStatelessActor].start
      (actor1 !! "hello").getOrElse("_") should equal("world")
      (actor1 !! "hello").getOrElse("_") should equal("world")

      val bytes = toBinary(actor1)
      val actor2 = fromBinary(bytes)
      actor2.start
      (actor2 !! "hello").getOrElse("_") should equal("world")
    }
  }
}

class MyActorWithDualCounter extends Actor {
  var count1 = 0
  var count2 = 0
  def receive = {
    case "hello" =>
      count1 = count1 + 1
      count2 = count2 + 1
      self.reply("world " + count1 + " " + count2)
  }
}

class MyActor extends Actor {
  var count = 0

  def receive = {
    case "hello" =>
      count = count + 1
      self.reply("world " + count)
  }
}

class MyStatelessActor extends Actor {
  def receive = {
    case "hello" =>
      self.reply("world")
  }
}
