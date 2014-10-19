package es.upm.fi.oeg.morph.streams.esper
import java.net.URI
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import org.slf4j.LoggerFactory
import akka.actor.actorRef2Scala
import akka.pattern.ask
import concurrent.duration._
import language.postfixOps
import es.upm.fi.oeg.morph.common.ParameterUtils.loadQuery
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.esper.EsperServer
import es.upm.fi.oeg.morph.esper.Event
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import es.upm.fi.oeg.morph.stream.esper.EsperEvaluator
import akka.actor.Props
import es.upm.fi.oeg.morph.stream.evaluate.RegisterQuery
import akka.util.Timeout
import es.upm.fi.oeg.morph.stream.evaluate.PullData
import scala.concurrent.ExecutionContext
import es.upm.fi.oeg.siq.sparql.SparqlResults
import es.upm.fi.oeg.morph.stream.evaluate.QueryId
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils
import es.upm.fi.oeg.morph.stream.evaluate.Data
import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.FlatSpec


class PolimiExecutionTest extends FlatSpec with BeforeAndAfterAll with Matchers  {
  private val logger= LoggerFactory.getLogger(this.getClass)
  implicit val timeout = Timeout(5 seconds) // needed for `?` below

  lazy val esper = new EsperServer
  //val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"))
  val eval=esper.system.actorOf(Props(new EsperEvaluator),"evaluator")
  //val eval = new EsperEvaluator
  
  private def polimi(q:String)=loadQuery("queries/polimi/"+q)
  private val polimiR2rml=new URI("mappings/polimitest.ttl")
  
  override def beforeAll() =	{
    //PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
    esper.startup
  }
  
  "polimi test"  should "succeed"  in{
    def data = List(       
		List("individualId"->1,"roomId"->1).toMap,
	    List("individualId"->2,"roomId"->1).toMap,
	    List("individualId"->1,"roomId"->2).toMap,
	    List("individualId"->2,"roomId"->2).toMap)
	def timestamps = List(1000, 2000, 9000, 3000)
    
    val demo = new PolimiStreamer("polimi",data,timestamps,new EsperProxy(esper.system)) 
    demo.schedule
    println("finish init")
    val qid= (eval ? RegisterQuery(polimi("polimi.sparql"),polimiR2rml))      
    var i = 0;
   import ExecutionContext.Implicits.global

    qid.onSuccess { case QueryId(id) =>
      logger.debug("created query with id: "+id)
    for(i <- 0 to 20){
	    val bindings= (eval ? PullData(id))  
	    bindings.onSuccess{case Data(sp:SparqlResults) =>
	      println(EvaluatorUtils.serializecsv(sp))
	      
	    }
	    Thread.sleep(1000)
    }
    }
   
   Thread.sleep(20000)
  }
  
  override def afterAll()={
    logger.debug("exiting now================================")
    esper.shutdown
  }

}



class PolimiStreamer(extent:String,data:List[Map[String,Int]],dataTimestamps:List[Int],proxy:EsperProxy)  {
  private val latestTime:Long=0
  private val logger = LoggerFactory.getLogger(getClass)

  def schedule{
    val eng=proxy.engine
    var i = 0
    import proxy.system.dispatcher
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