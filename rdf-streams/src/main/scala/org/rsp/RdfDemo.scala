package org.rsp

import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.vocabulary.VCARD
import org.apache.jena.riot.RDFLanguages
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFFormat
import org.apache.jena.riot.lang.PipedRDFIterator
import com.hp.hpl.jena.graph.Triple
import org.apache.jena.riot.lang.PipedTriplesStream
import scala.concurrent.Future
import scala.collection.JavaConversions._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.PoisonPill
import akka.routing.RoundRobinRouter

object RdfDemo {
  def createRdf={
    val pre="http://bit.com/"
    val m= ModelFactory.createDefaultModel
    
    val iter= new PipedRDFIterator[Triple]()
    val system=ActorSystem("rspSystem")
    val props=Props[RspFilter].withRouter(RoundRobinRouter(nrOfInstances=10))
    val filter=system.actorOf(props,"filter")
    val join=system.actorOf(Props[RspJoin],"join")
    val is=new PipedTriplesStream(iter)
    import scala.concurrent.ExecutionContext.Implicits.global 
    Future { 
      RDFDataMgr.parse(is,"file:///C:/Users/calbimon/data/lsd/4UT01_2004_8_10.n3")      
    }
    println ("Good times")
    val f=Future(iter)

    val t=f.map{a=>
      a.take(50)foreach{triple=>
        println(triple)
        filter ! triple        
      }      
    }
    println("do it")
    m.createResource(pre+"event1").addProperty(VCARD.NAME, "trevor")
    //m.write(System.out,RDFLanguages.TTL.getName)
    Thread.sleep(5000)
    system.shutdown
    
  }
  
  def main(args:Array[String]):Unit={
    createRdf
  }
}