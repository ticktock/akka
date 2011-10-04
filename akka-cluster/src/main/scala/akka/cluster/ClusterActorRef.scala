/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster

import akka.actor._
import akka.util._
import ReflectiveAccess._
import akka.routing._
import akka.event.EventHandler
import java.net.InetSocketAddress
import collection.immutable.Map
/**
 * ClusterActorRef factory and locator.
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object ClusterActorRef {

  import FailureDetectorType._
  import RouterType._

  def newRef(
    actorAddress: String,
    routerType: RouterType,
    failureDetectorType: FailureDetectorType,
    timeout: Long): ClusterActorRef = {

    val routerFactory: () ⇒ Router = routerType match {
      case Direct        ⇒ () ⇒ new DirectRouter
      case Random        ⇒ () ⇒ new RandomRouter
      case RoundRobin    ⇒ () ⇒ new RoundRobinRouter
      case LeastCPU      ⇒ sys.error("Router LeastCPU not supported yet")
      case LeastRAM      ⇒ sys.error("Router LeastRAM not supported yet")
      case LeastMessages ⇒ sys.error("Router LeastMessages not supported yet")
      case Custom        ⇒ sys.error("Router Custom not supported yet")
    }

    val failureDetectorFactory: (Map[InetSocketAddress, ActorRef]) ⇒ FailureDetector = failureDetectorType match {
      case RemoveConnectionOnFirstFailureLocalFailureDetector ⇒
        (connections: Map[InetSocketAddress, ActorRef]) ⇒ new RemoveConnectionOnFirstFailureLocalFailureDetector(connections.values)

      case RemoveConnectionOnFirstFailureRemoteFailureDetector ⇒
        (connections: Map[InetSocketAddress, ActorRef]) ⇒ new RemoveConnectionOnFirstFailureRemoteFailureDetector(connections)

      case CustomFailureDetector(implClass) ⇒
        (connections: Map[InetSocketAddress, ActorRef]) ⇒ FailureDetector.createCustomFailureDetector(implClass, connections)
    }

    new ClusterActorRef(
      RoutedProps()
        .withTimeout(timeout)
        .withRouter(routerFactory)
        .withFailureDetector(failureDetectorFactory),
      actorAddress)
  }

  /**
   * Finds the cluster actor reference that has a specific address.
   */
  def actorFor(address: String): Option[ActorRef] =
    Actor.registry.local.actorFor(Address.clusterActorRefPrefix + address)

  private[cluster] def createRemoteActorRef(actorAddress: String, inetSocketAddress: InetSocketAddress) = {
    RemoteActorRef(inetSocketAddress, actorAddress, Actor.TIMEOUT, None)
  }
}

/**
 * ActorRef representing a one or many instances of a clustered, load-balanced and sometimes replicated actor
 * where the instances can reside on other nodes in the cluster.
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
private[akka] class ClusterActorRef(props: RoutedProps, val address: String) extends AbstractRoutedActorRef(props) {

  import ClusterActorRef._

  ClusterModule.ensureEnabled()

  val addresses = Cluster.node.inetSocketAddressesForActor(address)

  EventHandler.debug(this,
    "Checking out cluster actor ref with address [%s] and router [%s] on [%s] connected to [\n\t%s]"
      .format(address, router, Cluster.node.remoteServerAddress, addresses.map(_._2).mkString("\n\t")))

  addresses foreach {
    case (_, address) ⇒ Cluster.node.clusterActorRefs.put(address, this)
  }

  val connections: FailureDetector = {
    val remoteConnections = (Map[InetSocketAddress, ActorRef]() /: addresses) {
      case (map, (uuid, inetSocketAddress)) ⇒
        map + (inetSocketAddress -> createRemoteActorRef(address, inetSocketAddress))
    }
    props.failureDetectorFactory(remoteConnections)
  }

  router.init(connections)

  def nrOfConnections: Int = connections.size

  private[akka] def failOver(from: InetSocketAddress, to: InetSocketAddress) {
    connections.failOver(from, to)
  }

  @volatile
  private var running: Boolean = false

  def isRunning: Boolean = running

  def isShutdown: Boolean = !running

  def stop() {
    synchronized {
      if (running) {
        running = false
      }
    }
  }

  if (isShutdown) {
    running = true
    Actor.registry.local.registerClusterActorRef(this)
  }
}
