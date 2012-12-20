package es.upm.fi.oeg.morph.stream.gsn
import com.weiglewilczek.slf4s.Logging
import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.scalatest.junit.ShouldMatchersForJUnit
import org.apache.log4j.PropertyConfigurator
import java.text.SimpleDateFormat
import java.net.URI
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import org.junit.Before
import org.junit.Test
import es.upm.fi.oeg.morph.common.ParameterUtils._
import es.upm.fi.oeg.morph.common.ParameterUtils



class SwissExQueryTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {
  PropertyConfigurator.configure(getClass.getClassLoader().getResource("config/log4j.properties"));
  private val df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val mappingUri=new URI("mappings/swissex.ttl")
  val props= ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/swissex.siq.properties"))
  val gsn=new QueryEvaluator(props)
  
  @Before def initialize() {}
   
  @Test def testObservations() {     
    val query = loadQuery("queries/swissex/observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  @Test def testCompleteObservations() {     
    val query = loadQuery("queries/swissex/complete-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }
  
  @Test def testObservationsHumidity() {     
    val query = loadQuery("queries/swissex/observations-humidity.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }
  

}