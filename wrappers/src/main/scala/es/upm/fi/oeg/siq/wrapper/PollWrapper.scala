package es.upm.fi.oeg.siq.wrapper
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import scala.Array.canBuildFrom
import scala.xml.Elem
import akka.actor.ReceiveTimeout
import akka.actor.Actor
import concurrent.duration._
import dispatch._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import scala.compat.Platform
import org.slf4j.LoggerFactory
import _root_.gsn.beans.DataField
import scala.util.Random
import java.util.UUID

trait PollWrapper {
  val logger=LoggerFactory.getLogger(this.getClass)
  //lazy val params=getActiveAddressBean  
  def configvals(key:String):String
 lazy val datasourcetype=configvals("sourcetype")
 lazy val datatype=configvals("type")
  lazy val rate=configvals("rate").toLong
  lazy val liveRate=rate
  lazy val dateTimeFormat=configvals("dateTimeFormat")
 lazy val url=configvals("url")
  lazy val urlparamvals=configvals("urlparams").split(',')
  lazy val urlparamnames=configvals("urlparamnames").split(',')
  lazy val urlparams=urlparamnames zip urlparamvals
  lazy val servicefields=configvals("servicefields").split(',')
  lazy val systemids=configvals("systemids").split(',')
  private lazy val systemnames=configvals("systemnames")//.split(',')
  lazy val idkeys={
    if (systemnames==null || systemnames.isEmpty) (systemids zip systemids).toMap 
    else (systemids zip systemnames.split(',')).toMap
  }
  
  def actorSystem:ActorSystem

  protected lazy val fieldNames=configvals("fields").split(',')  
  protected lazy val types=configvals("types").split(',')
  lazy val dataFields=fieldNames.zip(types).map(a=>new DataField(a._1,a._2,a._1))

      
  def postData(systemid:String,ts:Long,o:Observation):Unit
  
}

@Deprecated()
class EmtCaller(who:PollWrapper,stationid:String) extends Datasource(who,stationid){ 
  override def pollData={
      val svc = url("https://servicios.emtmadrid.es:8443/geo/servicegeo.asmx/getArriveStop")
      .addQueryParameter("idClient","")
      .addQueryParameter("passKey", "")
      .addQueryParameter("idStop", stationid)
      .addQueryParameter("statistics", " ").addQueryParameter("cultureInfo", " ")    
    val xml = Http(svc OK as.xml.Elem)
      val date=new Date
      val dats=(xml() \ "Arrive").map{arr=>    
        val data:Array[java.io.Serializable]=Array(stationid, 
               (arr \ "idLine").head.text,
               (arr \ "TimeLeftBus").head.text.toInt,
               (arr \ "DistanceBus").head.text.toInt)
        new Observation(date,data)
      }
      dats
    
      
  }
}

class Observation(val timestamp:Date,val values:Seq[Any]){
  lazy val serializable=values.toArray.map(_.asInstanceOf[java.io.Serializable])
}

