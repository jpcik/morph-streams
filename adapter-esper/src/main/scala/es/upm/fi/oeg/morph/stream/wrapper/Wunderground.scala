package es.upm.fi.oeg.morph.stream.wrapper

import scala.xml._
import com.sun.jersey.api.client.Client
import com.sun.jersey.core.util.MultivaluedMapImpl
import java.text.DateFormat
import java.util.Locale
import java.util.TimeZone
import scala.language.postfixOps

class Wunderground(id:String) {
  private val client = Client.create
  private val webResource = {
    val wundurl="http://api.wunderground.com/weatherstation/WXCurrentObXML.asp"
    val queryParams = new MultivaluedMapImpl
    queryParams.add("ID",id) 
    client.resource(wundurl).queryParams(queryParams)    
  }
  def getData={
	val res= webResource.get(classOf[String])
	val obs=Observation.fromXml(XML.loadString(res))
	println(obs)
	obs
  }
}

case class Location(full:String,neighborhood:String,
    city:String,state:String,latitude:Double,longitude:Double){
}

object Location{
  def fromXml(loc:Node)=
    Location(loc\"full" text,loc\"neighborhood" text,
        loc\"city" text,loc\"state" text,
        (loc\"latitude").text toDouble,
        (loc\"longitude").text toDouble)
}

object Observation{
  private val format=DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.MEDIUM,Locale.UK)
  format.setTimeZone(TimeZone.getTimeZone("GMT"))
  
  def fromXml(obs:Node)={
    val timeString:String=(obs\"observation_time_rfc822").text
    Observation(obs\"station_id" text,
        obs\"station_type" text,
        timeString,
        (obs\"temp_c").text toDouble,
        (obs\"relative_humidity").text toDouble,
        obs\"wind_dir" text,
        (obs\"wind_degrees").text toDouble,
        (obs\"wind_mph").text toDouble,
        (obs\"pressure_in").text toDouble,
        null,
        format.parse(timeString).getTime,
        Location.fromXml((obs\"location").head))
  }
}

case class Observation(stationId:String,stationType:String,observationTime:String,
    temperature:Double,relativeHumidity:Double,windDirection:String,windDegrees:Double,windSpeed:Double,airPressure:Double,
    credit:String,internalTime:Long,location:Location)
{	  
}