package es.upm.fi.oeg.morph.stream.esper
import scala.util.Random
import scala.compat.Platform
import es.upm.fi.oeg.morph.esper.EsperProxy
import concurrent.duration._
import scala.language.postfixOps

class DemoStreamer(stationid:String,extent:String,rate:Int,proxy:EsperProxy)   {
//extends EsperStreamer(extent,rate,esper){
  private var latestTime:Long=0
  private val rand=new Random
  def generateData={
    latestTime=5//data.internalTime
	Some(List("stationId"->stationid,//data.stationId,	      
	     "internalTime"->Platform.currentTime,////data.internalTime,
	     "observationTime"->rand.nextLong,
	     "airPressure"-> rand.nextDouble,
	     "temperature"->rand.nextDouble,
	     "relativeHumidity"->rand.nextDouble,
	     "timestamp"-> rand.nextDouble).toMap)	    
  }

  def schedule{
    val eng=proxy.engine
    import  proxy.system.dispatcher
    proxy.system.scheduler.schedule(0 seconds, 1 seconds){
      val tosend=generateData.get
      println("sending"+tosend.mkString("--"))
      eng ! es.upm.fi.oeg.morph.esper.Event(extent,tosend)}             
  }
  
}