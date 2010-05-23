package se.scalablesolutions.akka.kernel

import se.scalablesolutions.akka.config.Config._
import se.scalablesolutions.akka.config.Config.config
import se.scalablesolutions.akka.util.{Bootable, Logging}
import se.scalablesolutions.akka.actor.BootableActorLoaderService
import com.sun.grizzly.http.servlet.deployer.GrizzlyWebServerDeployer
import com.sun.grizzly.http.servlet.deployer.conf.{DeployableConfiguration,DeployerServerConfiguration}

import java.net.{URL,URLClassLoader}
import com.sun.grizzly.http.webxml.schema.WebApp

trait WebAppDeployer extends Bootable with Logging { self : BootableActorLoaderService =>
  @volatile private var deployer : Option[GrizzlyWebServerDeployer] = None
  abstract override def onLoad = {
    super.onLoad
    if(config.getBool("akka.webapps.service", false))
    {
      log.info("Starting up web app deployer")
      deployer = Some({
        val (deployDir,workDir,sharedDir) = HOME.map( (h) => ((h + "/deploy/webapps", h + "/deploy/work", h + "/deploy/shared")) ).getOrElse(throw new IllegalStateException("AKKA_HOME NOT SET!"))
        val conf = new DeployerServerConfiguration{
            port = config.getInt("akka.webapps.port", 9998)
            watchFolder = deployDir
            libraryPath = sharedDir
            cometEnabled = config.getBool("akka.webapps.comet", false)
            watchInterval = config.getInt("akka.webapps.watchinterval_sec", 10)
            websocketsEnabled = config.getBool("akka.webapps.websockets", false)
        }

        val d = new GrizzlyWebServerDeployer{
         override def deployApplications(conf : DeployerServerConfiguration) {
            //configureServer is unoverridable and sets WorkFolder to the wrong dir
            getWarDeployer.setWorkFolder(workDir)
            //Since I can only supply a libraryPath and not a CL, I need to hook this in
            //serverLibLoader = new URLClassLoader(Array[URL](),self.applicationLoader.getOrElse(Thread.currentThread.getContextClassLoader))
            //serverLibLoader = self.applicationLoader.get.asInstanceOf[URLClassLoader]
            super.deployApplications(conf)
          }
        }
        d launch conf
        d
      })
    }
  }

  abstract override def onUnload = {
    if(deployer.isDefined)
    {
      log.info("Shutting down web app deployer")
      deployer.foreach( _.stop )
      deployer = None
    }
    super.onUnload
  }
}