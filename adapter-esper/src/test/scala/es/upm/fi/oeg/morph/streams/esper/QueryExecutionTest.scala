package es.upm.fi.oeg.morph.streams.esper
import java.net.URI
import org.apache.log4j.PropertyConfigurator
import org.junit.Before
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import com.weiglewilczek.slf4s.Logging
import es.upm.fi.oeg.morph.common.ParameterUtils.loadQuery
import es.upm.fi.oeg.morph.stream.esper.DemoStreamer
import es.upm.fi.oeg.morph.stream.esper.EsperEngine
import es.upm.fi.oeg.morph.stream.esper.QueryMsg
import es.upm.fi.oeg.morph.stream.esper.RegisteredStream
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import es.upm.fi.oeg.morph.common.ParameterUtils
import es.upm.fi.oeg.morph.stream.esper.EsperAdapter
import org.junit.After
import scala.actors.SuspendActorControl
import scala.util.control.ControlThrowable

class Stream (val windspeed:Double) {
  private val inTime=System.nanoTime
  val time=inTime
}

class QueryExecutionTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {

  val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  val eval = new QueryEvaluator(props)
  val adapter=eval.adapter.asInstanceOf[EsperAdapter]
  val demo = new DemoStreamer("ISANGALL2","wunderground",1,adapter.esper) 
  
  @Before def setUpBeforeClass() 	{
    PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
    demo.start
  }
  

  @Test def basicPetternMatching{ 	 
    val qid=eval.registerQuery(loadQuery("queries/srbench/basic-pattern-matching.sparql"),
        new URI("mappings/srbench.ttl"))        
   Thread.sleep(5000)
   val bindings=eval.pull(qid)   
  }
  
  @Test def filterValue{ 	 
    val qid=eval.registerQuery(loadQuery("queries/srbench/filter-value.sparql"),
        new URI("mappings/srbench.ttl"))        
   Thread.sleep(7000)
   val bindings=eval.pull(qid)   
  }
  
  
  
  @After def destroyAfter(){
    logger.debug("exiting now================================")
    try{adapter.esper.stop
    }catch {case e:ControlThrowable=>println("finished")}
    try{demo.stop
    }catch {case e:ControlThrowable=>println("finished")}
  }

}