package es.upm.fi.oeg.morph.stream.gsn
import es.upm.fi.oeg.morph.common.ParameterUtils._
import java.net.URI
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.siq.sparql.SparqlResults
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils
import es.upm.fi.oeg.morph.stream.evaluate.Mapping
import org.scalatest.Matchers
import org.scalatest.FlatSpec

class CityBikesQueryTest extends FlatSpec with Matchers {
  //private val df=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  private val logger= LoggerFactory.getLogger(this.getClass)
  val mappingUri=Mapping(new URI("mappings/citybikes.ttl"))
  val gsn=new GsnAdapter("gsn")

  
  
  "Numeric value filter query " should "execute" in {     
    val query = loadQuery("queries/citybikes/numeric-value.sparql")
    logger.info(query)        
 
    gsn.executeQuery(query,mappingUri)
  }

  "Free slots and available bikes query " should "execute" in {     
    val query = loadQuery("queries/citybikes/free-slots-available-bikes.sparql")
    logger.info(query)        
    val sp=gsn.executeQuery(query,mappingUri).asInstanceOf[SparqlResults]
    logger.debug(EvaluatorUtils.serialize(sp))
  }

  "Available bikes observations query " should "execute" in  {     
    val query = loadQuery("queries/citybikes/availablebike-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  "Available bikes in one station query " should "execute" in {     
    val query = loadQuery("queries/citybikes/availablebike-onestation.sparql")
    logger.info(query)        
    val sp=gsn.executeQuery(query,mappingUri).asInstanceOf[SparqlResults]
    logger.debug(EvaluatorUtils.serialize(sp))
  }
  
  
  "Latest available bikes observation query " should "execute" in {     
    val query = loadQuery("queries/citybikes/latest-availablebike-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  "Latest available bikes observation in station query " should "execute" in {     
    val query = loadQuery("queries/citybikes/latest-availablebike-observations-station.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  "Latest available bikes observation construct query " should "execute" in {     
    val query = loadQuery("queries/citybikes/construct-latest-availablebike-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }
  
}