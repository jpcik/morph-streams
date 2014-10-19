package org.rsp

import akka.actor.Actor
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF

class RspFilter extends Actor{
    val j=context.actorSelection("/user/join")

  def receive ={
    case t:Triple => 
      //if (t.getPredicate==RDF.`type`.asNode)
        
        //println(self.path+" "+t)
        
      j ! t
  }
  override def preStart(): Unit = {
    
    println("we go")
  }
}