package es.upm.fi.oeg.morph.streams.esper
import es.upm.fi.oeg.morph.streams.wrapper.Wunderground

class WundergroundStreamer(stationid:String,extent:String,rate:Int,esper:EsperEngine) 
  extends EsperStreamer(extent,rate,esper){
  private val wund=new Wunderground(stationid)
  private var latestTime:Long=0
  override def generateData={
    val data=wund.getData
    if (data.internalTime>latestTime){
      latestTime=data.internalTime
	  Some(List("stationId"->data.stationId,	      
	     "internalTime"->data.internalTime,
	     "observationTime"->data.observationTime,
	     "airPressure"-> data.airPressure,
	     "temperature"->data.temperature,
	     "timestamp"-> data.windSpeed).toMap)	    
    }
	else None
  }
  
}