package org.rsp.cqels

import org.deri.cqels.engine.ExecContext
import org.deri.cqels.engine.RDFStream
import java.util.UUID
import scala.util.Random
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import concurrent.duration._
import com.hp.hpl.jena.graph.Triple
import akka.actor.ActorRef


class StreamProducer(dest:ActorRef) extends Actor{
  implicit val exe=context.system.dispatcher  
  val tick = context.system.scheduler.schedule(0 millis, 10 millis, self, "tick")
  def receive= {
    case "tick" => 
       val obs=omOwl("obs"+UUID.randomUUID)
       dest ! Seq(
       RDFdata(obs, omOwl.procedure ,omOwl("sensor1")),             
       RDFdata(obs,omOwl.observedProperty,weather.windSpeed),
       RDFdata(obs,omOwl.floatValue,Random.nextDouble.toString),
       RDFdata(obs,omOwl.timestamp ,System.currentTimeMillis.toString))   
  }
}

case class RDFdata(d:Tuple3[String,String,String])

object StreamGenerate {
def main(args:Array[String]):Unit={
    var stopall=false
    val sys=ActorSystem.create("cqels")  
    //val prod =sys.actorOf(Props[Producer], "prod")
    val context=new ExecContext("", false);
    (1 to 50000).foreach{i=>
    val stream = new TextStream(context, "http://deri.org/streams/rfid"+i)
      val tt=new Thread(stream)
    tt.start
    println("started"+i)
    }




  }
  


}
