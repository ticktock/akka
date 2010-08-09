package se.scalablesolutions.akka.amqp

/**
 *  Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

import se.scalablesolutions.akka.config.OneForOneStrategy
import com.rabbitmq.client.{ReturnListener, ShutdownListener, ConnectionFactory}
import java.lang.IllegalArgumentException
import se.scalablesolutions.akka.util.Logging
import se.scalablesolutions.akka.actor.{Actor, ActorRef}
import Actor._

/**
 * AMQP Actor API. Implements Connection, Producer and Consumer materialized as Actors.
 *
 * @see se.scalablesolutions.akka.amqp.ExampleSession
 *
 * @author Irmo Manie
 */
object AMQP {
  case class ConnectionParameters(
          host: String = ConnectionFactory.DEFAULT_HOST,
          port: Int = ConnectionFactory.DEFAULT_AMQP_PORT,
          username: String = ConnectionFactory.DEFAULT_USER,
          password: String = ConnectionFactory.DEFAULT_PASS,
          virtualHost: String = ConnectionFactory.DEFAULT_VHOST,
          initReconnectDelay: Long = 5000,
          connectionCallback: Option[ActorRef] = None)

  case class ChannelParameters(
          shutdownListener: Option[ShutdownListener] = None,
          channelCallback: Option[ActorRef] = None)

  case class ExchangeParameters(
          exchangeName: String,
          exchangeType: ExchangeType,
          exchangeDurable: Boolean = false,
          exchangeAutoDelete: Boolean = true,
          exchangePassive: Boolean = false,
          configurationArguments: Map[String, AnyRef] = Map())

  case class ProducerParameters(exchangeParameters: ExchangeParameters,
                                producerId: Option[String] = None,
                                returnListener: Option[ReturnListener] = None,
                                channelParameters: Option[ChannelParameters] = None)

  case class ConsumerParameters(exchangeParameters: ExchangeParameters,
                                routingKey: String,
                                deliveryHandler: ActorRef,
                                queueName: Option[String] = None,
                                queueDurable: Boolean = false,
                                queueAutoDelete: Boolean = true,
                                queuePassive: Boolean = false,
                                queueExclusive: Boolean = false,
                                selfAcknowledging: Boolean = true,
                                channelParameters: Option[ChannelParameters] = None) {
    if (queueDurable && queueName.isEmpty) {
      throw new IllegalArgumentException("A queue name is required when requesting a durable queue.")
    }
  }

  def newConnection(connectionParameters: ConnectionParameters = new ConnectionParameters): ActorRef = {
    val connection: ActorRef = supervisor.newConnection(connectionParameters)
    connection ! Connect
    connection
  }

  def newProducer(connection: ActorRef, producerParameters: ProducerParameters): ActorRef = {
    val producer: ActorRef = Actor.actorOf(new ProducerActor(producerParameters))
    connection.startLink(producer)
    producer ! Start
    producer
  }

  def newConsumer(connection: ActorRef, consumerParameters: ConsumerParameters): ActorRef = {
    val consumer: ActorRef = actorOf(new ConsumerActor(consumerParameters))
    consumer.startLink(consumerParameters.deliveryHandler)
    connection.startLink(consumer)
    consumer ! Start
    consumer
  }

  private val supervisor = new AMQPSupervisor

  class AMQPSupervisor extends Logging {
    class AMQPSupervisorActor extends Actor {
      import self._

      faultHandler = Some(OneForOneStrategy(5, 5000))
      trapExit = List(classOf[Throwable])

      def receive = {
        case _ => {} // ignore all messages
      }
    }

    private val supervisor = actorOf(new AMQPSupervisorActor).start

    def newConnection(connectionParameters: ConnectionParameters): ActorRef = {
      val connectionActor = actorOf(new FaultTolerantConnectionActor(connectionParameters))
      supervisor.startLink(connectionActor)
      connectionActor
    }
  }
}