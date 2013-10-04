package es.upm.fi.oeg.siq.wrapper

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import akka.actor.Actor
import es.upm.fi.oeg.morph.esper.Event
import es.upm.fi.oeg.morph.esper.EsperServer

object Runner {
  def main(args:Array[String]):Unit={
     val actorSystem=ActorSystem("demorunner",ConfigFactory.load.getConfig("demosystem"))
     val engine=actorSystem.actorOf(Props[DemoEngine],"EsperEngine")
     //val esper= new EsperServer
     //esper.startup
     
     
     new ApiWrapper("emt",actorSystem)
     
     Thread.sleep(10000)
     //esper.shutdown
     
  }
  
  class DemoEngine extends Actor{
    def receive={
      case e:Event=>println(e)
    }
  }
}

