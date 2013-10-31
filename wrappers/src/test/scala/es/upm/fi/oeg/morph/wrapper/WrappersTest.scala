package es.upm.fi.oeg.morph.wrapper

import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.junit.Test
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import es.upm.fi.oeg.siq.wrapper.ApiWrapper
import akka.actor.Actor
import es.upm.fi.oeg.morph.esper.Event

class WrappersTest extends JUnitSuite with ShouldMatchersForJUnit {
  val actorSystem=ActorSystem("demorunner",ConfigFactory.load.getConfig("demosystem"))
  val engineWM=actorSystem.actorOf(Props[DemoEngine],"EsperEngineWeatherMap")
  val engineCB=actorSystem.actorOf(Props[DemoEngine],"EsperEngineCitybikes")
     
  @Test def randomWrapper:Unit={
    new ApiWrapper("hl7",actorSystem)
    
    Thread.sleep(10000)
  }
  
  @Test def csvWrapper:Unit={
    new ApiWrapper("social",actorSystem)
    Thread.sleep(15000)
  }      

  @Test def jsonWrapper:Unit={
    new ApiWrapper("weathermap",actorSystem)
    Thread.sleep(5000)
  }      

  @Test def citybikesWrapper:Unit={
    new ApiWrapper("citybikes",actorSystem)
    Thread.sleep(5000)
  }      

}


class DemoEngine extends Actor{
  def name=self.path.name
  def receive={
    case e:Event=>
      validate(e)
      println(e)
  }
  
  def validate(e:Event)={
    if (name.endsWith("WeatherMap")){
      assert(e.attributes.size==5)
    }    
  }
}