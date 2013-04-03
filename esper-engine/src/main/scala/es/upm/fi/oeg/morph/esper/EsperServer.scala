package es.upm.fi.oeg.morph.esper
import akka.actor.Actor
import com.espertech.esper.client.Configuration
import com.espertech.esper.client.EPServiceProviderManager
import collection.JavaConversions._
import akka.kernel.Bootable
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import org.slf4j.LoggerFactory

class EsperServer extends Bootable {
  val logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Start Esper Actor Server")
  val system = ActorSystem("esperkernel",ConfigFactory.load.getConfig("espereng"))
 
  def startup = {
    system.actorOf(Props[EsperEngine],"EsperEngine") ! Ping("Startup message")
  }
 
  def shutdown = system.shutdown
}
