package es.upm.fi.oeg.morph.stream.esper
import scala.util.Random
import scala.compat.Platform

class DemoStreamer(stationid:String,extent:String,rate:Int,esper:EsperEngine)   
extends EsperStreamer(extent,rate,esper){
  private var latestTime:Long=0
  private val rand=new Random
  override def generateData={
      latestTime=5//data.internalTime
	  Some(List("stationId"->stationid,//data.stationId,	      
	     "internalTime"->Platform.currentTime,////data.internalTime,
	     "observationTime"->"",
	     "airPressure"-> rand.nextDouble,
	     "temperature"->rand.nextDouble,
	     "timestamp"-> rand.nextDouble).toMap)	    
  }
  
}