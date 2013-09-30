package es.upm.fi.oeg.morph.stream.gsn
import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import java.text.SimpleDateFormat
import java.net.URI
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import org.junit.Before
import org.junit.Test
import es.upm.fi.oeg.morph.common.ParameterUtils._
import es.upm.fi.oeg.morph.common.ParameterUtils
import org.slf4j.LoggerFactory
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import akka.pattern.ask
import es.upm.fi.oeg.morph.stream.evaluate.ExecuteQuery
import akka.util.Timeout
import concurrent.duration._
import es.upm.fi.oeg.siq.sparql.SparqlResults
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils


class SwissExQueryTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  private val logger= LoggerFactory.getLogger(this.getClass)
  //val actorSystem=ActorSystem("swissex",ConfigFactory.load.getConfig("gsnakka"))
  val gsn=new GsnAdapter("gsn1")
  //private val df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val mappingUri=new URI("mappings/swissex.ttl")
  //implicit val timeout = Timeout(5 second)
  
  @Before def initialize() {}
   
  @Test def testObservations() {     
    val query = loadQuery("queries/swissex/observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
    
  }

  @Test def testCompleteObservations() {     
    val query = loadQuery("queries/swissex/complete-observations.sparql")
    logger.info(query)        
    val sp=gsn.executeQuery(query,mappingUri).asInstanceOf[SparqlResults]
   //logger.debug(EvaluatorUtils.serialize(sp))

  }
  
  @Test def testObservationsHumidity() {     
    val query = loadQuery("queries/swissex/observations-humidity.sparql")
    logger.info(query)        
    val sp= gsn.executeQuery(query,mappingUri).asInstanceOf[SparqlResults]
    logger.debug(EvaluatorUtils.serialize(sp))
  }
  

}