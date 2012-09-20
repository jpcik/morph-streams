package es.upm.fi.oeg.morph.streams.esper
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers
import org.scalatest.junit.ShouldMatchersForJUnit
import com.weiglewilczek.slf4s.Logging
import org.junit.Before
import org.apache.log4j.PropertyConfigurator
import es.upm.fi.oeg.morph.common.ParameterUtils
import com.espertech.esper.client.Configuration
import com.google.common.collect.Maps
import scala.util.Random
import com.espertech.esper.client.EPServiceProviderManager
import org.junit.Test
import scala.actors.Actor
import scala.actors.Actor._
import com.espertech.esper.client.EPRuntime
import collection.JavaConversions._
import com.espertech.esper.event.map.MapEventBean
import java.net.URL
import es.upm.fi.oeg.morph.streams.wrapper.Wunderground

class Stream (val windspeed:Double) {
  private val inTime=System.nanoTime
  val time=inTime
}

class QueryExecutionTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {

  val props = ParameterUtils.load(getClass.getResourceAsStream("/config/config_memoryStore.esper.properties"))
  val esper= new EsperEngine
	
  @Before def setUpBeforeClass() 	{
    PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
    //val wun=new Wunderground
	esper.start			
  }

  @Test def testQuery{ 	 
    new WundergroundStreamer("ISANGALL2","milford",1,esper).start

  	esper ! RegisteredStream("milford",100)
  	Thread.sleep(6000)
  	  	
    val q="Select m1.Hs from milfordwinn as m1"
  	println(q)
  	esper ! QueryMsg(q)
  	println("now  here")
	Thread.sleep(2000)

  	esper ! QueryMsg(q)
  	Thread.sleep(2000)
  	esper ! QueryMsg(q)
  	Thread.sleep(100)
  }
}