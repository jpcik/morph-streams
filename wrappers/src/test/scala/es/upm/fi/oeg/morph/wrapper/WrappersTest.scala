package es.upm.fi.oeg.morph.wrapper

import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.junit.Test
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import es.upm.fi.oeg.siq.wrapper.Runner.DemoEngine
import es.upm.fi.oeg.siq.wrapper.ApiWrapper

class WrappersTest extends JUnitSuite with ShouldMatchersForJUnit {
  val actorSystem=ActorSystem("demorunner",ConfigFactory.load.getConfig("demosystem"))
  val engine=actorSystem.actorOf(Props[DemoEngine],"EsperEngine")
  //val esper= new EsperServer
  //esper.startup
     
  @Test def randomWrapper:Unit={
    new ApiWrapper("hl7",actorSystem)
    Thread.sleep(10000)
  }
  
  @Test def csvWrapper:Unit={
    new ApiWrapper("social",actorSystem)
    Thread.sleep(15000)
  }      
}