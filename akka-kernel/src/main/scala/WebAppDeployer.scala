/*package se.scalablesolutions.akka.servlet

import se.scalablesolutions.akka.config.Config._
import se.scalablesolutions.akka.config.Config.config
import se.scalablesolutions.akka.util.{Bootable, Logging}

import com.sun.grizzly.http.servlet.deployer.GrizzlyWebServerDeployer
import com.sun.grizzly.http.servlet.deployer.conf.DeployableConfiguration
import com.sun.grizzly.http.servlet.deployer.conf.DeployerServerConfiguration

class WebAppDeployer extends Bootable with Logging { self : BootableActorLoaderService =>
  @volatile private var deployer : Option[GrizzlyWebServerDeployer] = None
  abstract override def onLoad = {
	super.onLoad
	if(config.getBoolean("akka.webappdeployer.service", false))
	{
	  log.info("Starting up web app deployer")
      deployer = Some({
        val deployDir = HOME.map( _ + "/deploy").getOrElse(throw new IllegalStateException("AKKA_HOME NOT SET!"))
        val conf = new DeployerServerConfiguration

        conf.watchFolder = deployDir
        conf.cometEnabled = config.getBoolean("akka.webappdeployer.comet", true)
        conf.watchInterval = config.getInt("akka.webappdeployer.watchinterval_sec", 120) // 2 min
	
	    val d = new GrizzlyWebServerDeployer
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
}*/