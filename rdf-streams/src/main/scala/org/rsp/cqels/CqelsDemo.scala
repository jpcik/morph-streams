package org.rsp.cqels

import org.deri.cqels.engine.ExecContext
import org.deri.cqels.engine.ContinuousListener
import org.deri.cqels.data.Mapping
import collection.JavaConversions._
import org.deri.cqels.engine.RDFStream
import java.util.UUID
import org.deri.cqels.engine.ConstructListener
import com.hp.hpl.jena.graph.Triple
import scala.collection.mutable.ArrayBuffer
import collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor._
import scala.util.Random

object CqelsDemo {

  def execute(context:ExecContext,cl:ConstructListener)={

    //context.loadDefaultDataset("{DIRECTORY TO LOAD DEFAULT DATASET}");
    //context.loadDataset("{URI OF NAMED GRAPH}", "{DIRECTORY TO LOAD NAMED GRAPH}");
    val queryString =s"""CONSTRUCT {?s <http://pop.org/prod> ?o}  
      where { 
        stream <http://deri.org/streams/rfid> [RANGE 1000ms] 
      {?s <${omOwl.timestamp}> ?o}}"""
    

    val selQuery=context.registerConstruct(queryString)
    
    selQuery.register(cl)
  }
    
  def cqelsListener(context:ExecContext,prod:ActorRef)={
    new ConstructListener(context){
      val buffer:ArrayBuffer[Data]=new ArrayBuffer
      var count=0
                 
      def update(triples:java.util.List[Triple]):Unit={
        count+=1
        val z=new Data(triples)
        prod ! z
      } 
    }
  }
  
  
  def main(args:Array[String]):Unit={
    var stopall=false
    val sys=ActorSystem.create("cqels")  
    val prod =sys.actorOf(Props[Producer], "prod")
    val context=new ExecContext("", false);
    val stream = new TextStream(context, "http://deri.org/streams/rfid")
    implicit val exe=sys.dispatcher
    sys.scheduler.scheduleOnce(1 minute){
      stream.stop
      sys.stop(prod)
      stopall=true
    }
    val tt=new Thread(stream)
    tt.start

    val cl=cqelsListener(context,prod)
    val ini=System.currentTimeMillis
    execute(context,cl)
    while(!stopall){
      Thread.sleep(1000)
    }
    //stream.stop
    //tt.stop
    println(s"${cl.count} in 10 sec" )
    tt.stop
  }
  
}

class TextStream(context:ExecContext,uri:String) 
  extends RDFStream(context,uri) with Runnable{
  var stopp=false
  var sleep=0

  override def stop():Unit= {
    stopp=true
  }
 
  def run():Unit= {
    while ((!stopp))   {
      //(1 to 10000).foreach{f=>
      try{
        val obs=omOwl("obs"+UUID.randomUUID)
        stream(obs, omOwl.procedure ,omOwl("sensor1"))             
        stream(obs,omOwl.observedProperty,weather.windSpeed)
        stream(obs,omOwl.floatValue,Random.nextDouble.toString)
        stream(obs,omOwl.timestamp ,System.currentTimeMillis.toString)        
      }
      catch {
        case _:Throwable => stopp=true
      }
                                
      if(sleep>0){
        Thread.sleep(sleep)
      }
    }
  }
        
}

class Ontology {
  val prefix=""
  def inst(value:String)=prefix+value
  def apply(value:String)=inst(value)
}

object omOwl extends Ontology{
  override val prefix="http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#"
  val procedure=prefix+"procedure"
  val observedProperty=prefix+"observedProperty"
  val floatValue=prefix+"floatValue"
  val timestamp=prefix+"timestamp"
    
}

object weather{
  val prefix="http://knoesis.wright.edu/ssw/ont/weather.owl#"
  val windSpeed=prefix+"windSpeed"  
    
}

 
