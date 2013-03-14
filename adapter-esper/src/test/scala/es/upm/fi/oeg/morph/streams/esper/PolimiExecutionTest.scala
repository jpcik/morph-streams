package es.upm.fi.oeg.morph.streams.esper
import java.net.URI

import org.apache.log4j.PropertyConfigurator
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import org.slf4j.LoggerFactory

import com.weiglewilczek.slf4s.Logging

import akka.actor.actorRef2Scala
import akka.util.duration.intToDurationInt
import es.upm.fi.oeg.morph.common.ParameterUtils.loadQuery
import es.upm.fi.oeg.morph.common.ParameterUtils
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.esper.EsperServer
import es.upm.fi.oeg.morph.esper.Event
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator


class PolimiExecutionTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {
  
  lazy val esper = new EsperServer
  val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  val eval = new QueryEvaluator(props,esper.system)
  
  private def polimi(q:String)=loadQuery("queries/polimi/"+q)
  private val polimiR2rml=new URI("mappings/polimitest.ttl")
  
  @Before def setUpBeforeClass() 	{
    PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
    esper.startup
  }
  
  @Test def polimiTest(){
    def data = List(       
		List("individualId"->1,"roomId"->1).toMap,
	    List("individualId"->2,"roomId"->1).toMap,
	    List("individualId"->1,"roomId"->2).toMap,
	    List("individualId"->2,"roomId"->2).toMap)
	def timestamps = List(1000, 2000, 9000, 3000)
    
    val demo = new PolimiStreamer("polimi",data,timestamps,new EsperProxy(esper.system)) 
    demo.schedule
    println("finish init")
    val qid=eval.registerQuery(polimi("polimi.sparql"),polimiR2rml)        
    var i = 0;
    for(i <- 0 to 20){
	    val bindings=eval.pull(qid)  
	    Thread.sleep(1000)
    }
  }
  
  @After def after(){
    logger.debug("exiting now================================")
    esper.shutdown
  }

}



class PolimiStreamer(extent:String,data:List[Map[String,Int]],dataTimestamps:List[Int],proxy:EsperProxy)  {
  private var latestTime:Long=0
  private var logger = LoggerFactory.getLogger("es.upm.fi.oeg.morph.stream.esper.PolimiStreamer")

  def schedule{
    val eng=proxy.engine
    var i = 0
    proxy.system.scheduler.scheduleOnce(1 seconds){
      dataTimestamps.foreach{delay =>
        	println("sleeping for "+delay)
        	Thread.sleep(delay)
        	var row=data(i)
        	i=i+1
        	logger.info("----------------------> inserting: "+row);
        	eng ! Event(extent,row)
      }
    }
  }
  
}