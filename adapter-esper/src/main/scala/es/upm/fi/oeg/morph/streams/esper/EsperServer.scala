package es.upm.fi.oeg.morph.streams.esper

import akka.actor._
import akka.routing.RoundRobinRouter
import akka.kernel.Bootable
import com.typesafe.config.ConfigFactory

class EsperServer extends Actor{
  def receive={
    case QueryMsg(query)=>      
      println(query)
      //val engine=new EsperEngine
      //engine.start()
  }
}

//trait Msg
//case class Query(query:String) extends Msg


class EsperKernel extends Bootable {
  val system = ActorSystem("hellokernel",ConfigFactory.load.getConfig("calculator"))
 
  def startup = {
    system.actorOf(Props[EsperServer],"EsperServer") ! QueryMsg("bidbiban!!!")
  }
 
  def shutdown = {
    system.shutdown()
  }
}