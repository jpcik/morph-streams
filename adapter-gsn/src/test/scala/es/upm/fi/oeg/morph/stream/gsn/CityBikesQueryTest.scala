package es.upm.fi.oeg.morph.stream.gsn
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import org.scalatest.junit.JUnitSuite
import org.apache.log4j.PropertyConfigurator
import es.upm.fi.oeg.morph.common.ParameterUtils._
import java.net.URI
import org.junit.Before
import org.junit.Test
import com.sun.jersey.api.client.Client
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import es.upm.fi.oeg.morph.stream.gsn.wrapper.BikeObservation
import java.text.SimpleDateFormat
import java.util.Calendar
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import org.junit.Ignore
import org.slf4j.LoggerFactory

class CityBikesQueryTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  PropertyConfigurator.configure(getClass.getClassLoader().getResource("config/log4j.properties"));
  private val df=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  private val logger= LoggerFactory.getLogger(this.getClass)
  val mappingUri=new URI("mappings/citybikes.ttl")
  val gsn=new QueryEvaluator(null)
  
  @Before def initialize() {}
 
  @Test def testDateParse(){
     val ts="2012-09-20T21:51:34.559"
       //"2012-06-14 14:42:19.699"
     val datetime=df.parse(ts)
     val c=Calendar.getInstance
     c.setTime(datetime)
     println(c.getTime())

  }
  @Test def testCityBikesApi(){
    val client=Client.create
    val webResource=client.resource("http://api.citybik.es/bizi.json")
    val s=webResource.get(classOf[String])
    val data=new Gson().fromJson(s,classOf[Array[BikeObservation]])
    data.foreach(println)
  }
  
  @Test def testNumericValue() {     
    val query = loadQuery("queries/citybikes/numeric-value.sparql")
    logger.info(query)        
 
    gsn.executeQuery(query,mappingUri)
  }

  @Test def testFreeBikesAvSlots() {     
    val query = loadQuery("queries/citybikes/free-slots-available-bikes.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  @Test def testAvailableBikeObservations() {     
    val query = loadQuery("queries/citybikes/availablebike-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  @Test def testLatestAvailableBikeObservations() {     
    val query = loadQuery("queries/citybikes/latest-availablebike-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  @Ignore@Test def testLatestAvailableBikeObservationsStation() {     
    val query = loadQuery("queries/citybikes/latest-availablebike-observations-station.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }

  @Test def testConstructLatestAvailableBikeObservations() {     
    val query = loadQuery("queries/citybikes/construct-latest-availablebike-observations.sparql")
    logger.info(query)        
    gsn.executeQuery(query,mappingUri)
  }
  
}