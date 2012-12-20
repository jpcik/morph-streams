package es.upm.fi.oeg.morph.esper
import akka.actor.Actor
import com.espertech.esper.client.Configuration
import com.espertech.esper.client.EPServiceProviderManager
import collection.JavaConversions._
import akka.kernel.Bootable
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props

class EsperServer extends Bootable {
  println("We are about to start all ============================")
  val system = ActorSystem("esperkernel",ConfigFactory.load.getConfig("espereng"))
 
  def startup = {
    system.actorOf(Props[EsperEngine],"EsperEngine") ! Ping("bidbibaaaan!!!")
  }
 
  def shutdown = {
    system.shutdown()
  }
}
