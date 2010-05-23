package sample.lift

import se.scalablesolutions.akka.actor.{Transactor, Actor}
import se.scalablesolutions.akka.config.ScalaConfig._
import se.scalablesolutions.akka.stm.TransactionalState
import se.scalablesolutions.akka.persistence.cassandra.CassandraStorage
import Actor._

import java.lang.Integer
import java.nio.ByteBuffer

class SimpleService extends Transactor {
  case object Tick
  private val KEY = "COUNTER"
  private var hasStartedTicking = false
  private lazy val storage = TransactionalState.newMap[String, Integer]

  def count = (self !! Tick).getOrElse(<h1>Error in counter</h1>)

  def receive = {
    case Tick => if (hasStartedTicking) {
      val counter = storage.get(KEY).get.asInstanceOf[Integer].intValue
      storage.put(KEY, new Integer(counter + 1))
      self.reply(<h1>Tick: {counter + 1}</h1>)
    } else {
      storage.put(KEY, new Integer(0))
      hasStartedTicking = true
      self.reply(<h1>Tick: 0</h1>)
    }
  }
}

class PersistentSimpleService extends Transactor {

  case object Tick
  private val KEY = "COUNTER"
  private var hasStartedTicking = false
  private lazy val storage = CassandraStorage.newMap

  def count = (self !! Tick).getOrElse(<h1>Error in counter</h1>)

  def receive = {
    case Tick => if (hasStartedTicking) {
      val bytes = storage.get(KEY.getBytes).get
      val counter = ByteBuffer.wrap(bytes).getInt
      storage.put(KEY.getBytes, ByteBuffer.allocate(4).putInt(counter + 1).array)
      self.reply(<success>Tick:{counter + 1}</success>)
    } else {
      storage.put(KEY.getBytes, ByteBuffer.allocate(4).putInt(0).array)
      hasStartedTicking = true
      self.reply(<success>Tick: 0</success>)
    }
  }
}
