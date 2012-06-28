package es.upm.fi.oeg.morph.stream.gsn
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.prop.Checkers
import com.weiglewilczek.slf4s.Logging
import org.scalatest.junit.JUnitSuite
import org.apache.log4j.PropertyConfigurator
import es.upm.fi.dia.oeg.common.ParameterUtils._
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting
import java.net.URI
import org.junit.Before
import org.junit.Test
import com.sun.jersey.api.client.Client
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import es.upm.fi.oeg.morph.stream.gsn.wrapper.BikeObservation
import es.upm.fi.oeg.morph.stream.gsn.wrapper.BikeObservation
import java.text.SimpleDateFormat
import java.util.Calendar


class CityBikesQueryTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {
  PropertyConfigurator.configure(getClass.getClassLoader().getResource("config/log4j.properties"));
  private val df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  
  @Before def initialize() {}
 
  @Test def testDateParse(){
     val ts="2012-06-14 14:42:19.699"
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
    val gsn=new GsnAdapter
    gsn.executeQuery(query)
  }

  @Test def testFreeBikesAvSlots() {     
    val query = loadQuery("queries/citybikes/free-slots-available-bikes.sparql")
    logger.info(query)        
    val gsn=new GsnAdapter
    gsn.executeQuery(query)
  }

  @Test def testAvailableBikeObservations() {     
    val query = loadQuery("queries/citybikes/availablebike-observations.sparql")
    logger.info(query)        
    val gsn=new GsnAdapter
    gsn.executeQuery(query)
  }

}