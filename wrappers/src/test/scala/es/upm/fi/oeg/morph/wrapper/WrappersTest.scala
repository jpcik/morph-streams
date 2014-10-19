package es.upm.fi.oeg.morph.wrapper

import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import es.upm.fi.oeg.siq.wrapper.ApiWrapper
import akka.actor.Actor
import es.upm.fi.oeg.morph.esper.Event
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class WrappersTest  extends FlatSpec with Matchers  {
  val actorSystem=ActorSystem("demorunner",ConfigFactory.load.getConfig("demosystem"))
  val engineEmt=actorSystem.actorOf(Props[DemoEngine],"EsperEngineEmt")
  val engineWM=actorSystem.actorOf(Props[DemoEngine],"EsperEngineWeatherMap")
  val engineCB=actorSystem.actorOf(Props[DemoEngine],"EsperEngineCitybikes")
  val engineHL7=actorSystem.actorOf(Props[DemoEngine],"EsperEngineHL7")
     
  "randomWrapper" should "wrap" in{
    new ApiWrapper("hl7",actorSystem)
    
    Thread.sleep(11000)
  }
  
  "csvWrapper" should "wrap" in{
    new ApiWrapper("social",actorSystem)
    Thread.sleep(15000)
  }      

  "jsonWrapper" should "wrap" in{
    new ApiWrapper("weathermap",actorSystem)
    Thread.sleep(5000)
  }      

  "citybikesWrapper" should "wrap" in{
    new ApiWrapper("citybikes",actorSystem)
    Thread.sleep(5000)
  }      

  "xmlrestWrapper" should "wrap" in{
    new ApiWrapper("emt",actorSystem)
    Thread.sleep(10000)
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