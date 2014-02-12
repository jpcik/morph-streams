package es.upm.fi.oeg.morph.stream.gsn
import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import java.text.SimpleDateFormat
import java.net.URI
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
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
import es.upm.fi.oeg.morph.stream.evaluate.Mapping
import org.scalatest.Matchers
import org.scalatest.FlatSpec


class SwissExQueryTest extends FlatSpec with Matchers {
  private val logger= LoggerFactory.getLogger(this.getClass)
  //val actorSystem=ActorSystem("swissex",ConfigFactory.load.getConfig("gsnakka"))
  val gsn=new GsnAdapter("gsn1")
  //private val df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val mappingUri=Mapping(new URI("mappings/swissex.ttl"))
  //implicit val timeout = Timeout(5 second)
     
  "testObservations" should "execute" in {     
    val query = loadQuery("queries/swissex/observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
    
  }

  "testCompleteObservations" should "execute" in{     
    val query = loadQuery("queries/swissex/complete-observations.sparql")
    logger.info(query)        
    val sp=gsn.executeQuery(query,mappingUri).asInstanceOf[SparqlResults]
   //logger.debug(EvaluatorUtils.serialize(sp))

  }
  
  "testObservationsHumidity" should "execute" in {     
    val query = loadQuery("queries/swissex/observations-humidity.sparql")
    logger.info(query)        
    val sp= gsn.executeQuery(query,mappingUri).asInstanceOf[SparqlResults]
    logger.debug(EvaluatorUtils.serialize(sp))
  }
  

}