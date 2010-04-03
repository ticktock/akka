/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.persistence.cassandra

import se.scalablesolutions.akka.serialization.Serializable
import se.scalablesolutions.akka.actor.annotation.transactionrequired
import se.scalablesolutions.akka.actor.annotation.prerestart
import se.scalablesolutions.akka.actor.annotation.postrestart
import se.scalablesolutions.akka.actor.annotation.inittransactionalstate
import se.scalablesolutions.akka.actor.annotation.oneway
import se.scalablesolutions.akka.stm._
import se.scalablesolutions.akka.kernel.Kernel

import com.google.inject.Inject

object PersistenceManager {
  @volatile private var isRunning = false
  def init = if (!isRunning) {
    Kernel.startRemoteService
    isRunning = true
  }
}

class PersistentClasher {
  private var state: PersistentMap = _

  @inittransactionalstate
  def init = state = CassandraStorage.newMap
  def getState(key: String) = state.get(key).get
  def setState(key: String, msg: String) = state.put(key, msg)
  def clash = state.put("clasher", "was here")
}

@serializable class PersistentFailer  {
  def fail = throw new RuntimeException("expected")
}

@transactionrequired
class PersistentStateful {
  private lazy val mapState = CassandraStorage.newMap
  private lazy val vectorState = CassandraStorage.newVector
  private lazy val refState = CassandraStorage.newRef
  
  def getMapState(key: String) = {
    val bytes = mapState.get(key.getBytes).get
    new String(bytes, 0, bytes.length)
  }
  
   def getVectorState(index: Int) {
     val bytes = vectorState.get(key.getBytes).get
     new String(bytes, 0, bytes.length)
  }
   def getVectorLength = vectorState.length
   def getRefState = {
    if (refState.isDefined) {
      val bytes = refState.get(key.getBytes).get
      new String(bytes, 0, bytes.length)
    } else throw new IllegalStateException("No such element")
  }
  
   def setMapState(key: String, msg: String) = mapState.put(key.getBytes, msg.getBytes)
   def setVectorState(msg: String) = vectorState.add(msg.getBytes)
   def setRefState(msg: String) = refState.swap(msg.getBytes)
   def success(key: String, msg: String) = {
    mapState.put(key.getBytes, msg.getBytes)
    vectorState.add(msg.getBytes)
    refState.swap(msg.getBytes)
  }

   def failure(key: String, msg: String, failer: PersistentFailer) = {
    mapState.put(key.getBytes, msg.getBytes)
    vectorState.add(msg.getBytes)
    refState.swap(msg.getBytes)
    failer.fail()
     msg
  }

   def success(key: String, msg: String, nested: PersistentStatefulNested) = {
    mapState.put(key.getBytes, msg.getBytes)
    vectorState.add(msg.getBytes)
    refState.swap(msg.getBytes)
    nested.success(key, msg)
     msg
  }

   def failure(key: String, msg: String, nested: PersistentStatefulNested, failer: PersistentFailer) = {
    mapState.put(key.getBytes, msg.getBytes)
    vectorState.add(msg.getBytes)
    refState.swap(msg.getBytes)
    nested.failure(key, msg, failer)
     msg
  }
}

@transactionrequired
class PersistentStatefulNested extends PersistentStateful
