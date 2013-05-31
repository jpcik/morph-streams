package es.upm.fi.oeg.morph.stream.rewriting

import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.esper.EsperServer
import es.upm.fi.oeg.siq.tools.ParameterUtils
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import java.net.URI
import org.junit.Before
import es.upm.fi.oeg.morph.stream.esper.DemoStreamer
import es.upm.fi.oeg.morph.esper.EsperProxy
import org.junit.Test
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils
import es.upm.fi.oeg.morph.stream.evaluate.StreamReceiver
import es.upm.fi.oeg.siq.sparql.SparqlResults

class ReasoningExecutionTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)
  lazy val esper=new EsperServer
  val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  val eval = new QueryEvaluator(props,esper.system)
  
  private def ssn(q:String)=ParameterUtils.loadQuery("queries/ssn/"+q)
  private val srbenchR2rml=new URI("mappings/srbench.ttl")
  
  @Before def setUpBeforeClass() {
    //PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
    esper.startup()
    val demo = new DemoStreamer("ISANGALL2","wunderground",1,new EsperProxy(esper.system)) 
    demo.schedule
    println("finish init")
  }
  

  @Test def filterUriDiff{
    val res= new ResultsReceiver(0,0) 
    val qid=eval.listenToQuery(ssn("q1.sparql"),srbenchR2rml,res)        
    Thread.sleep(4000)
    //val bindings=eval.pull(qid)
    //logger.debug(EvaluatorUtils.serialize(bindings))
  }

}

class ResultsReceiver (start:Long,rate:Long) extends StreamReceiver{
  private val logger=LoggerFactory.getLogger(this.getClass)
  
  override def receiveData(s:SparqlResults){    
    logger.info(EvaluatorUtils.serialize(s))
  }
  
}