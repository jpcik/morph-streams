package org.rsp.cqels

import akka.actor._
import scala.collection.mutable.ArrayBuffer

class Producer extends Actor {
  val buffer = new ArrayBuffer[Data]
  val receiver=context.actorOf(Props(new Receiver(self)))
  var count=0
  var demand=0
  private def dynamic(d:Data)={
      if (buffer.isEmpty && count<demand){
        receiver ! d
        count+=1
      }
      else {
        //println("buffering")
        buffer += d
        count=0
      }
    
  }

  private def normal(d:Data)={
        receiver ! d
        count+=1    
  }

  def receive ={
    case d:Data=>
      dynamic(d)
    case "Get" =>
      (1 to demand).foreach{i=>
      if (buffer.size>0){
        println("now from buffer")
        val n=buffer.remove(0)
        receiver ! n
      }

      }
    case Demand(d)=>
      println(s"=========================Finally got fdemand $d")
      demand=d
  }
}