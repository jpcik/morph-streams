package es.upm.fi.oeg.siq.wrapper
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import scala.Array.canBuildFrom
import scala.xml.Elem
import gsn.beans.DataField
import gsn.wrappers.AbstractWrapper
import akka.actor.ReceiveTimeout
import akka.actor.Actor
import concurrent.duration._
import dispatch._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import gsn.beans.StreamElement
import scala.compat.Platform
import org.slf4j.LoggerFactory

class PollWrapper extends AbstractWrapper {
  val logger=LoggerFactory.getLogger(this.getClass)
  lazy val params=getActiveAddressBean  
  lazy val datatype=params.getPredicateValue("type")
  lazy val rate=params.getPredicateValue("rate").toLong
  lazy val liveRate=rate
  lazy val dateTimeFormat=params.getPredicateValue("dateTimeFormat")
  lazy val url=params.getPredicateValue("url")
  lazy val urlparamvals=params.getPredicateValue("urlparams").split(',')
  lazy val urlparamnames=params.getPredicateValue("urlparamnames").split(',')
  lazy val urlparams=urlparamnames zip urlparamvals
  lazy val servicefields=params.getPredicateValue("servicefields").split(',')
  lazy val user=params.getPredicateValue("user")
  lazy val key=params.getPredicateValue("key")
  lazy val systemids=params.getPredicateValue("systemids").split(',')
  private lazy val systemnames=params.getPredicateValue("systemnames")//.split(',')
  lazy val idkeys={
    if (systemnames==null) (systemids zip systemids).toMap 
    else (systemids zip systemnames.split(',')).toMap
  }
  
  protected val actorSystem=ActorSystem("wrap",ConfigFactory.load.getConfig("restapiwrapper"))

  private lazy val fieldNames=params.getPredicateValue("fields").split(',')  
  private lazy val types=params.getPredicateValue("types").split(',')
  lazy val dataFields=fieldNames.zip(types).map(a=>new DataField(a._1,a._2,a._1))
      
  def postData(systemid:String,ts:Long,o:Observation){
    logger.trace("post vals "+o.serializable.size)
    //val ser =o.values.asInstanceOf[Array[java.io.Serializable]]
    postStreamElement(ts,o.serializable)
    //postStreamElement(new StreamElement(dataFields,o.serializable),ts)
    
        //Array[java.io.Serializable](systemid,o.id,o.name,o.timestamp,o.bikes,o.free))
    //println("data posted: "+systemid+"."+o)                       
  }
  
  override def initialize={
    setName("PollWrapper")
    setUsingRemoteTimestamp(true)
    true
  }
  override def getOutputFormat=dataFields 
  override def dispose {}
  override def getWrapperName="PollWrapper"
    
  override def run{
    systemids.foreach{systemid=>

      val sc=new EmtCaller(this,systemid)
      //sc.start
    }
    while (isActive){
      Thread.sleep(liveRate)
      println("still alive")
    }
  }
}

@Deprecated()
class EmtCaller(who:PollWrapper,stationid:String) extends SystemCaller(who,stationid){ 
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

abstract class SystemCaller(who:PollWrapper,systemid:String) extends Actor{
  //private val df=new SimpleDateFormat(who.dateTimeFormat)//"yyyy-MM-dd HH:mm:ss.SSS")
  def pollData:Seq[Observation]
  
  def callRest{
    try{  
      val obs=pollData
      val start=Platform.currentTime
      var i=0
      obs.foreach{o=>        
        val datetime= o.timestamp
        val c=Calendar.getInstance
        c.setTime(datetime)
        who.postData(systemid,start+i,o)
        i+=1
      }
    } catch {case e:Exception=>e.printStackTrace}
  }
  
  context.setReceiveTimeout(who.rate millisecond) 
  def receive={
    case ReceiveTimeout=>callRest
  }
}