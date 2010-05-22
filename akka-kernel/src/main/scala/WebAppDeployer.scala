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
	if(config.getBool("akka.webappdeployer.service", false))
	{
	  log.info("Starting up web app deployer")
      deployer = Some({
        val (deployDir,workDir) = HOME.map( (h) => ((h + "/deploy/webapps", h + "/deploy/work")) ).getOrElse(throw new IllegalStateException("AKKA_HOME NOT SET!"))
        val conf = new DeployerServerConfiguration{
	        port = config.getInt("akka.webappdeployer.port", 9998)
        	watchFolder = deployDir
	        cometEnabled = config.getBool("akka.webappdeployer.comet", false)
	        watchInterval = config.getInt("akka.webappdeployer.watchinterval_sec", 10) // 2 min
	        websocketsEnabled = config.getBool("akka.webappdeployer.websockets", true)
        }

        
	    val d = new GrizzlyWebServerDeployer{
         override def deployApplications(conf : DeployerServerConfiguration) {
            getWarDeployer.setWorkFolder(workDir) //configureServer is unoverridable and sets WorkFolder to the wrong dir
            serverLibLoader = new URLClassLoader(Array[URL](),self.applicationLoader.getOrElse(Thread.currentThread.getContextClassLoader))
            super.deployApplications(conf)
          }
	    }
        d launch conf
        log.info("workdir: " + d.getWorkFolder)
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