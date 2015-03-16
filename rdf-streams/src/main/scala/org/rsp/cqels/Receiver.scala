package org.rsp.cqels

import akka.actor._
import com.hp.hpl.jena.graph.Triple
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent._
import scala.concurrent.duration._

class Receiver(producer:ActorRef) extends Actor {
  val delay=10
  var demand=5000
  implicit val exe=context.system.dispatcher  
  val tick = context.system.scheduler.schedule(0 millis, 1000 millis, self, "tick")
  var count=0
  var total=0
  
  override def postStop() = {
     tick.cancel
     println(s"total $total")
  }

  def receive ={
    case "tick" =>
      if (demand-count > 0 ){
        producer ! "Get"//Demand(demand-count)
        count=0
      }
      producer ! Demand(demand)
    case Start => 

    case d:Data =>      
      //println(s" $count $demand")
      if (count>demand){
        context.stop(self)
        throw new OutOfMemoryError("terrible things")        
      }
      else{

      Future{blocking{process(d)}}
      
      }
                  count += 1

  }
  
  def process(d:Data)={
    //println("processing dara")
    total +=1
    Thread.sleep(delay)    
    if (count>0) count-=1
  }
}

case class Data(data:Seq[Triple])
case object Start
case class Demand(size:Int)