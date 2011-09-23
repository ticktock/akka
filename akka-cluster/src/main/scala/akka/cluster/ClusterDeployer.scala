/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster

import akka.actor.DeploymentConfig._
import akka.actor._
import akka.event.EventHandler
import akka.config.Config
import akka.util.Switch
import akka.util.Helpers._
import akka.cluster.storage.{ MissingDataException, DataExistsException, BadVersionException, StorageException }

import coordination.CoordinationLockListener

import scala.collection.immutable.Seq

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import storage.DataExistsException

/**
 * A ClusterDeployer is responsible for deploying a Deploy.
 *
 * FIXME Document: what does Deploy mean?
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object ClusterDeployer extends ActorDeployer {
  val clusterName = Cluster.name
  val nodeName = Config.nodename
  val clusterPath = "/%s" format clusterName

  val deploymentPath = clusterPath + "/deployment"
  val deploymentAddressPath = deploymentPath + "/%s"

  val deploymentCoordinationPath = clusterPath + "/deployment-coordination"
  val deploymentInProgressLockPath = deploymentCoordinationPath + "/in-progress"
  val isDeploymentCompletedInClusterLockPath = deploymentCoordinationPath + "/completed" // should not be part of basePaths

  val basePaths = List(clusterPath, deploymentPath, deploymentCoordinationPath, deploymentInProgressLockPath)

  private val isConnected = new Switch(false)
  private val deploymentCompleted = new CountDownLatch(1)

  private val coordination = Cluster.newCoordinationClient()

  private val deploymentInProgressLockListener = new CoordinationLockListener {
    def lockAcquired() {
      EventHandler.debug(this, "Clustered deployment started")
    }

    def lockReleased() {
      EventHandler.debug(this, "Clustered deployment completed")
      deploymentCompleted.countDown()
    }
  }

  private val deploymentInProgressLock = coordination.getLock(deploymentInProgressLockPath, deploymentInProgressLockListener)

  private val systemDeployments: List[Deploy] = Nil

  def shutdown() {
    isConnected switchOff {
      // undeploy all
      try {
        for {
          child ← coordination.getChildren(deploymentPath)
          deployment ← coordination.read[Deploy](deploymentAddressPath.format(child))
        } coordination.delete(deploymentAddressPath.format(deployment.address))

        invalidateDeploymentInCluster()
      } catch {
        case e: Exception ⇒
          handleError(new DeploymentException("Could not undeploy all deployment data in ZooKeeper due to: " + e))
      }

      // shut down ZooKeeper client
      coordination.close()
      EventHandler.info(this, "ClusterDeployer shut down successfully")
    }
  }

  def lookupDeploymentFor(address: String): Option[Deploy] = ensureRunning {
    LocalDeployer.lookupDeploymentFor(address) match { // try local cache
      case Some(deployment) ⇒ // in local cache
        deployment
      case None ⇒ // not in cache, check cluster
        val deployment =
          try {
            Some(coordination.read[Deploy](deploymentAddressPath.format(address)))
          } catch {
            case e: MissingDataException ⇒ None
            case e: Exception ⇒
              EventHandler.warning(this, e.toString)
              None
          }
        deployment foreach (LocalDeployer.deploy(_)) // cache it in local cache
        deployment
    }
  }

  def fetchDeploymentsFromCluster: List[Deploy] = ensureRunning {
    val addresses =
      try {
        coordination.getChildren(deploymentPath).toList
      } catch {
        case e: MissingDataException ⇒ List[String]()
      }
    val deployments = addresses map { address ⇒
      coordination.read[Deploy](deploymentAddressPath.format(address))
    }
    EventHandler.info(this, "Fetched deployment plan from cluster [\n\t%s\n]" format deployments.mkString("\n\t"))
    deployments
  }

  private[akka] def init(deployments: Seq[Deploy]) {
    isConnected switchOn {
      EventHandler.info(this, "Initializing cluster deployer")

      basePaths foreach { path ⇒
        try {
          ignore[DataExistsException](coordination.createPath(path))
          EventHandler.debug(this, "Created ZooKeeper path for deployment [%s]".format(path))
        } catch {
          case e ⇒
            val error = new DeploymentException(e.toString)
            EventHandler.error(error, this)
            throw error
        }
      }

      val allDeployments = deployments ++ systemDeployments

      if (!isDeploymentCompletedInCluster) {
        if (deploymentInProgressLock.lock()) {
          // try to be the one doing the clustered deployment
          EventHandler.info(this, "Pushing deployment plan cluster [\n\t" + allDeployments.mkString("\n\t") + "\n]")
          allDeployments foreach (deploy(_)) // deploy
          markDeploymentCompletedInCluster()
          deploymentInProgressLock.unlock() // signal deployment complete

        } else {
          deploymentCompleted.await(30, TimeUnit.SECONDS) // wait until deployment is completed by other "master" node
        }
      }

      // fetch clustered deployments and deploy them locally
      fetchDeploymentsFromCluster foreach (LocalDeployer.deploy(_))
    }
  }

  private[akka] def deploy(deployment: Deploy) {
    ensureRunning {
      LocalDeployer.deploy(deployment)
      deployment match {
        case Deploy(_, _, _, _, Local) | Deploy(_, _, _, _, _: Local) ⇒ //TODO LocalDeployer.deploy(deployment)??
        case Deploy(address, recipe, routing, _, _) ⇒ // cluster deployment
          /*TODO recipe foreach { r ⇒
            Deployer.newClusterActorRef(() ⇒ Actor.actorOf(r.implementationClass), address, deployment)
          }*/
          val path = deploymentAddressPath.format(address)
          try {
            ignore[DataExistsException](coordination.createPath(path))
            coordination.forceUpdate(path, deployment)
          } catch {
            case e: NullPointerException ⇒
              handleError(new DeploymentException("Could not store deployment data [" + deployment + "] in ZooKeeper since client session is closed"))
            case e: Exception ⇒
              handleError(new DeploymentException("Could not store deployment data [" + deployment + "] in ZooKeeper due to: " + e))
          }
      }
    }
  }

  private def markDeploymentCompletedInCluster() {
    ignore[DataExistsException](coordination.createPath(isDeploymentCompletedInClusterLockPath))
  }

  private def isDeploymentCompletedInCluster = coordination.exists(isDeploymentCompletedInClusterLockPath)

  // FIXME in future - add watch to this path to be able to trigger redeployment, and use this method to trigger redeployment
  private def invalidateDeploymentInCluster() {
    ignore[MissingDataException](coordination.delete(isDeploymentCompletedInClusterLockPath))
  }

  private def ensureRunning[T](body: ⇒ T): T = {
    if (isConnected.isOn) body
    else throw new IllegalStateException("ClusterDeployer is not running")
  }

  private[akka] def handleError(e: Throwable): Nothing = {
    EventHandler.error(e, this, e.toString)
    throw e
  }
}
