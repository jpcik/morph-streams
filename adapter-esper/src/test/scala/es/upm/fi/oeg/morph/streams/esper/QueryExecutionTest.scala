package es.upm.fi.oeg.morph.streams.esper
import java.net.URI
import es.upm.fi.oeg.morph.common.ParameterUtils.loadQuery
import es.upm.fi.oeg.morph.common.ParameterUtils
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.esper.EsperServer
import es.upm.fi.oeg.morph.stream.esper.DemoStreamer
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.esper.EsperEvaluator
import akka.actor.Props
import akka.pattern.ask
import es.upm.fi.oeg.morph.stream.evaluate.RegisterQuery
import akka.util.Timeout
import concurrent.duration._
import language.postfixOps
import es.upm.fi.oeg.morph.stream.esper.EsperAdapter
import es.upm.fi.oeg.morph.stream.evaluate.Mapping
import es.upm.fi.oeg.siq.sparql.SparqlResults
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.esper.CreateWindow
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class QueryExecutionTest extends FlatSpec with BeforeAndAfterAll with Matchers   {
  private val logger= LoggerFactory.getLogger(this.getClass)
  //implicit val timeout = Timeout(5 seconds) // needed for `?` below

  lazy val esper=new EsperServer
  
  val eval =new EsperAdapter(esper.system)
  
  private def srbench(q:String)=loadQuery("queries/srbench/"+q)
  private val srbenchR2rml=Mapping(new URI("mappings/srbench.ttl"))
  
  override def beforeAll() ={
    //PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
    esper.startup()
    val proxy=new EsperProxy(esper.system)
    val demo = new DemoStreamer("ISANGALL2","wunderground",1,proxy) 
    demo.schedule
    //proxy.engine ! CreateWindow("wunderground","wundwind","60")
    println("finish init")
  }
 
  "registerRemove" should "remove" in{    
    val qid=eval.registerQuery(srbench("filter-uri-diff.sparql"),srbenchR2rml)        
    Thread.sleep(4000)
    val bindings=eval.pull(qid)
    
    logger.debug(EvaluatorUtils.serialize(bindings))
  }


  "filterUriDiff" should "filter URi" in{    
    val qid=eval.registerQuery(srbench("filter-uri-diff.sparql"),srbenchR2rml)        
    Thread.sleep(4000)
    val bindings=eval.pull(qid)
    logger.debug(EvaluatorUtils.serialize(bindings))
  }

   "joinPatternObjects" should "join pattern in object" in{    
    val qid=eval.registerQuery(srbench("join-pattern-objects.sparql"),srbenchR2rml)        
    Thread.sleep(8000)
    val bindings=eval.pull(qid)   
  }

  "basicPatternMatching" should "pattern match" in{    
    val qid=eval.registerQuery(srbench("basic-pattern-matching.sparql"),srbenchR2rml)        
    Thread.sleep(8000)
    val bindings=eval.pull(qid)   
  }
  
  
  
  "filterValue" should "filter values " in{ 	 
    val qid=eval.registerQuery(srbench("filter-value.sparql"),srbenchR2rml)        
    Thread.sleep(7000)
    val bindings=eval.pull(qid)   
    logger.debug(EvaluatorUtils.serialize(bindings))
  }    
  
  "joinPatternMatching" should "join" in{ 	 
    val qid=eval.registerQuery(srbench("join-pattern-matching.sparql"),srbenchR2rml)        
    Thread.sleep(7000)
    val bindings=eval.pull(qid)   
  }    

  "optionalPatternMatching" should "match pattern opt" in{ 	 
    val qid=eval.registerQuery(srbench("optional-pattern-matching.sparql"),srbenchR2rml)        
    Thread.sleep(7000)
    val bindings=eval.pull(qid)   
  }    

  "optionalJoinObservations" should "join optional" in{ 	 
    val qid=eval.registerQuery(srbench("optional-join-observations.sparql"),srbenchR2rml)        
    Thread.sleep(7000)
    val bindings=eval.pull(qid)   
  }    

  "filterUriValue" should "filter by uri value" in{ 	 
    val qid=eval.registerQuery(srbench("filter-uri-value.sparql"),srbenchR2rml)        
    Thread.sleep(4000)
    val bindings=eval.pull(qid)
    EvaluatorUtils.serialize(bindings)
  }    

  "variablePredicate" should "execute with var predicate" in{ 	 
    val qid=eval.registerQuery(srbench("variable-predicate.sparql"),srbenchR2rml)        
    Thread.sleep(4000)
    val bindings=eval.pull(qid)
    EvaluatorUtils.serialize(bindings)
  }    

  "maxAggregate" should "agg max" in{ 	 
    val qid=eval.registerQuery(srbench("max-aggregate.sparql"),srbenchR2rml)        
    Thread.sleep(4000)
    val bindings=eval.pull(qid)
    logger.info(EvaluatorUtils.serialize(bindings))
  }    

 
  "staticJoin" should "join with static" in{ 	 
    val qid=eval.registerQuery(srbench("static-join.sparql"),srbenchR2rml)        
    Thread.sleep(10000)
    val bindings=eval.pull(qid)
    logger.info(EvaluatorUtils.serialize(bindings))
  }    

  
  override def afterAll()={
    logger.debug("exiting now================================")
    esper.shutdown
  }

}