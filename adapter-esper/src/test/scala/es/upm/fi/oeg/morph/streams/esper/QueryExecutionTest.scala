package es.upm.fi.oeg.morph.streams.esper
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers
import org.scalatest.junit.ShouldMatchersForJUnit
import com.weiglewilczek.slf4s.Logging
import org.junit.Before
import org.apache.log4j.PropertyConfigurator
import es.upm.fi.dia.oeg.common.ParameterUtils
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

class Stream (val windspeed:Double) {
  private val inTime=System.nanoTime
  val time=inTime
}

class EsperStreamer(extents:Seq[String],epRuntime:EPRuntime) extends Actor with Logging{
  val rate = 1 
  val r=new Random		
  def act(){
	while (true) {
	  logger.debug("sending ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
	  extents.foreach{extent=>
		val map = Maps.newHashMap[String,Object]
		val time = r.nextLong();
		map.put("DateTime", r.nextLong:java.lang.Long)
		map.put("Hs", r.nextDouble:java.lang.Double)
		map.put("timestamp", time:java.lang.Long)
		try
		  epRuntime.sendEvent(map, extent)
		catch {case e:Exception=>logger.info("get out")}
	  }
	  Thread.sleep(1000);
    }
  }
}

case class QueryMsg(query:String)

class EsperServer extends Actor with Logging{
  val extents = Array("envdata_milford","envdata_perranporth","envdata_pevenseybay")
      /*
			"envdata_goodwin","envdata_torbay","envdata_rustington","envdata_bidefordbay",
			"envdata_folkestone","envdata_boscombe","envdata_penzance","envdata_weymouth")*/
  val configuration = new Configuration
  //configuration.addEventType("Stream", classOf[Stream])
  extents.foreach{ext=>
	val map = Maps.newHashMap[String,Object]
	map.put("DateTime",  classOf[Long])
	map.put("Hs", classOf[Double])
	map.put("timestamp", classOf[Long])        
	configuration.addEventType(ext, map)
  }
  val epService = EPServiceProviderManager.getProvider("benchmark", configuration)
  val epAdministrator = epService.getEPAdministrator
  val epRuntime = epService.getEPRuntime

  new EsperStreamer(extents,epRuntime).start
  
  def act(){
    val st=epAdministrator.createEPL("create window milford.win:keepall() as select Hs from envdata_milford")
    epAdministrator.createEPL("insert into milford select Hs from envdata_milford")
    	  //val res=epRuntime.executeQuery(query)
    	  //println(res.getArray().size)
    	  //res.iterator().foreach(println(_))
    	  //res.getArray.foreach(e=>println(e.get("windspeed")))

    while (true){
      receive {
    	case QueryMsg(query)=>
    	  logger.debug("received: "+query)
    	  /*
    	  val sst=epAdministrator.createEPL("select * from envdata_milford")
    	  val it=sst.safeIterator
    	  while (it.hasNext){
    	    val vv=it.next
    	    logger.debug(vv.get("Hs").toString)
    	  }
    	  it.close*/
    	  //sst.destroy
    	  logger.debug("afterwards")
   
    	  //val st=epAdministrator.createEPL("create window milford.win:keepall() as select * from envdata_milford")
    	  
    	  val res=epRuntime.executeQuery(query)
    	  //println(res.execute().getArray().size)
    	  
    	  //res.iterator().foreach(println(_))
    	  res.getArray.foreach(e=>println(e.get("m1.Hs")))
      } 
    	  
   }                                            
  }    
}


class QueryExecutionTest extends JUnitSuite with ShouldMatchersForJUnit with Checkers with Logging {

  val props = ParameterUtils.load(getClass.getResourceAsStream("/config/config_memoryStore.esper.properties"))
  val esper= new EsperServer

			/*
			"envdata_rye","envdata_westonbay","envdata_haylingisland","envdata_hornsea",
			"envdata_rhylflats","envdata_chesil","envdata_westbay","envdata_looebay",
			"envdata_startbay","envdata_sandownbay","envdata_minehead","envdata_seaford","envdata_bracklesham",
			"envdata_lymington_tide","envdata_hernebay_tide","envdata_deal_tide","envdata_teignmouthpier_tide",
			"envdata_swanagepier_tide","envdata_sandownpier_tide","envdata_westbaypier_tide",
			"envdata_deal_met","envdata_hernebay_met","envdata_looebay_met","envdata_arunplatform_met","envdata_swanagepier_met", 
			"envdata_sandownpier_met",	"envdata_weymouth_met","envdata_westbaypier_met","envdata_teignmouthpier_met", 
			"envdata_folkestone_met", "envdata_lymington_met",	"envdata_worthing_met"};*/
	
  @Before def setUpBeforeClass() 	{
    PropertyConfigurator.configure(getClass.getResource("/config/log4j.properties"))
	esper.start			
  }

  @Test def testQuery{ 	 
  	Thread.sleep(6000)
  	
    val q="Select m1.Hs from milford as m1"
  	println(q)
  	esper ! QueryMsg(q)
  	println("now  here")
	Thread.sleep(2000)

  	esper ! QueryMsg(q)
  	Thread.sleep(20000)
  	esper ! QueryMsg(q)
  }
}