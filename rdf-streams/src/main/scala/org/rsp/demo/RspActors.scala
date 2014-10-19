package org.rsp.demo

import akka.actor._
import akka.actor.SupervisorStrategy._
import akka.routing.RoundRobinRouter
import scala.concurrent.duration._
import collection.JavaConversions._
import com.hp.hpl.jena.graph.Triple
import org.rsp._
import scala.concurrent.Future

object RspActors {
  def execute={ 
    val system=ActorSystem("rspSystem")
    val source=system.actorOf(Props[StrSource])
    source ! Start
    Thread.sleep(10000)
    system.shutdown
    System.exit(0)    
  }
  
  def main(args:Array[String]):Unit={
    execute
  }
}

object Start

class StrSource extends Actor{
  import context.dispatcher
  val suervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3,
    withinTimeRange = 5 seconds) {
    case _: TripleException => SupervisorStrategy.Restart
  }
  def receive ={
    case Start => 
      val props=Props[StrFilter].withRouter(RoundRobinRouter(nrOfInstances=3,
          supervisorStrategy=this.suervisorStrategy ))
      val filter=context.actorOf(props,"filter")

      val f=Future(RspStreams.streamTriples)
      f map{a=>a foreach{triple=>
        filter ! triple        
      }}      
  }
}

class StrFilter extends Actor{
  val j=context.actorSelection("/user/join")

  def receive ={
    case t:Triple => 
      println(t.getObject)
      if (t.getObject.getLiteralValue.toString.toDouble < 0.5){      
        println("yes")
        throw new TripleException("bad")
      }
      //j ! t
  }
  override def preStart: Unit = {    
    println("start filter")
  }
}

class TripleException(msg:String) extends Exception(msg)