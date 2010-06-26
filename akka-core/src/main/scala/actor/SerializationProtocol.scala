/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.actor

trait FromBinary[T <: Actor] {
  def fromBinary(bytes: Array[Byte], act: T): Unit
}

trait ToBinary[T <: Actor] {
  def toBinary(t: T): Array[Byte]
}

trait Format[T <: Actor] extends FromBinary[T] with ToBinary[T]

import se.scalablesolutions.akka.config.{AllForOneStrategy, OneForOneStrategy, FaultHandlingStrategy}
import se.scalablesolutions.akka.config.ScalaConfig._
import se.scalablesolutions.akka.stm.global._
import se.scalablesolutions.akka.stm.TransactionManagement._
import se.scalablesolutions.akka.stm.TransactionManagement
import se.scalablesolutions.akka.remote.protocol.RemoteProtocol._
import se.scalablesolutions.akka.serialization.Serializer

import com.google.protobuf.ByteString

object ActorSerialization {

  def fromBinary[T <: Actor](bytes: Array[Byte])(implicit format: Format[T]): ActorRef = {
    fromBinaryToLocalActorRef(bytes, format)
  }

  def toBinary[T <: Actor](a: ActorRef)(implicit format: Format[T]): Array[Byte] = { 
    val protocol = toSerializedActorRefProtocol(a, format)
    protocol.toByteArray
  }

  private def toSerializedActorRefProtocol[T <: Actor](a: ActorRef, format: Format[T]): SerializedActorRefProtocol = {
    val lifeCycleProtocol: Option[LifeCycleProtocol] = {
      def setScope(builder: LifeCycleProtocol.Builder, scope: Scope) = scope match {
        case Permanent => builder.setLifeCycle(LifeCycleType.PERMANENT)
        case Temporary => builder.setLifeCycle(LifeCycleType.TEMPORARY)
      }
      val builder = LifeCycleProtocol.newBuilder
      a.lifeCycle match {
        case Some(LifeCycle(scope, None)) => 
          setScope(builder, scope)
          Some(builder.build)
        case Some(LifeCycle(scope, Some(callbacks))) =>
          setScope(builder, scope)
          builder.setPreRestart(callbacks.preRestart)
          builder.setPostRestart(callbacks.postRestart)
          Some(builder.build)
        case None => None
      }
    }

    val originalAddress = AddressProtocol.newBuilder
      .setHostname(a.homeAddress.getHostName)
      .setPort(a.homeAddress.getPort)
      .build

    val builder = SerializedActorRefProtocol.newBuilder
      .setUuid(a.uuid)
      .setId(a.id)
      .setActorClassname(a.actorClass.getName)
      .setOriginalAddress(originalAddress)
      .setIsTransactor(a.isTransactor)
      .setTimeout(a.timeout)
    builder.setActorInstance(ByteString.copyFrom(format.toBinary(a.actor.asInstanceOf[T])))
    a.serializer.foreach(s => builder.setSerializerClassname(s.getClass.getName))
    lifeCycleProtocol.foreach(builder.setLifeCycle(_))
    a.supervisor.foreach(s => builder.setSupervisor(s.toRemoteActorRefProtocol))
    // FIXME: how to serialize the hotswap PartialFunction ??
    //hotswap.foreach(builder.setHotswapStack(_))
    builder.build
  }

  private def fromBinaryToLocalActorRef[T <: Actor](bytes: Array[Byte], format: Format[T]): ActorRef =
    fromProtobufToLocalActorRef(SerializedActorRefProtocol.newBuilder.mergeFrom(bytes).build, format, None)

  private def fromProtobufToLocalActorRef[T <: Actor](protocol: SerializedActorRefProtocol, format: Format[T], loader: Option[ClassLoader]): ActorRef = {
    Actor.log.debug("Deserializing SerializedActorRefProtocol to LocalActorRef:\n" + protocol)
  
    val serializer = if (protocol.hasSerializerClassname) { 
      val serializerClass =
        if (loader.isDefined) loader.get.loadClass(protocol.getSerializerClassname)
        else Class.forName(protocol.getSerializerClassname)
      Some(serializerClass.newInstance.asInstanceOf[Serializer])
    } else None
    
    val lifeCycle =
      if (protocol.hasLifeCycle) {
        val lifeCycleProtocol = protocol.getLifeCycle
        val restartCallbacks =
          if (lifeCycleProtocol.hasPreRestart || lifeCycleProtocol.hasPostRestart) 
            Some(RestartCallbacks(lifeCycleProtocol.getPreRestart, lifeCycleProtocol.getPostRestart))
          else None
        Some(if (lifeCycleProtocol.getLifeCycle == LifeCycleType.PERMANENT) LifeCycle(Permanent, restartCallbacks)
             else if (lifeCycleProtocol.getLifeCycle == LifeCycleType.TEMPORARY) LifeCycle(Temporary, restartCallbacks)
             else throw new IllegalStateException("LifeCycle type is not valid: " + lifeCycleProtocol.getLifeCycle))
      } else None

    val supervisor =
      if (protocol.hasSupervisor)
        Some(fromProtobufToRemoteActorRef(protocol.getSupervisor, loader))
      else None
      
    val hotswap =
      if (serializer.isDefined && protocol.hasHotswapStack) Some(serializer.get
        .fromBinary(protocol.getHotswapStack.toByteArray, Some(classOf[PartialFunction[Any, Unit]]))
        .asInstanceOf[PartialFunction[Any, Unit]])
      else None

    val ar = new LocalActorRef(
      protocol.getUuid,
      protocol.getId,
      protocol.getActorClassname,
      protocol.getActorInstance.toByteArray,
      protocol.getOriginalAddress.getHostname,
      protocol.getOriginalAddress.getPort,
      if (protocol.hasIsTransactor) protocol.getIsTransactor else false,
      if (protocol.hasTimeout) protocol.getTimeout else Actor.TIMEOUT,
      lifeCycle,
      supervisor,
      hotswap,
      loader.getOrElse(getClass.getClassLoader), // TODO: should we fall back to getClass.getClassLoader?
      serializer,
      protocol.getMessagesList.toArray.toList.asInstanceOf[List[RemoteRequestProtocol]])
      format.fromBinary(protocol.getActorInstance.toByteArray, ar.actor.asInstanceOf[T])
      ar
  }

  private def fromProtobufToRemoteActorRef(protocol: RemoteActorRefProtocol, loader: Option[ClassLoader]): ActorRef = {
    Actor.log.debug("Deserializing RemoteActorRefProtocol to RemoteActorRef:\n" + protocol)
    RemoteActorRef(
      protocol.getUuid,
      protocol.getActorClassname,
      protocol.getHomeAddress.getHostname,
      protocol.getHomeAddress.getPort,
      protocol.getTimeout,
      loader)
  }
}
