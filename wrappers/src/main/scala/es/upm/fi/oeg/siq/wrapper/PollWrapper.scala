package es.upm.fi.oeg.siq.wrapper
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import scala.Array.canBuildFrom
import scala.xml.Elem
import es.emt.wsdl.ServiceGEOSoap12Bindings
import gsn.beans.DataField
import gsn.wrappers.AbstractWrapper
import akka.actor.ReceiveTimeout
import akka.actor.Actor
import concurrent.duration._

class PollWrapper extends AbstractWrapper {
  
  lazy val params=getActiveAddressBean  
  lazy val rate=params.getPredicateValue("rate").toLong
  lazy val liveRate=rate
  lazy val dateTimeFormat=params.getPredicateValue("dateTimeFormat")
  lazy val url=params.getPredicateValue("url")
  lazy val user=params.getPredicateValue("user")
  lazy val key=params.getPredicateValue("key")
  lazy val systemids=params.getPredicateValue("systemids").split(',')
  private lazy val fieldNames=params.getPredicateValue("fields").split(',')
  private lazy val types=params.getPredicateValue("types").split(',')
  private lazy val dataFields=fieldNames.zip(types).map(a=>new DataField(a._1,a._2,a._1))
      
  def postData(systemid:String,ts:Long,o:Observation){
    //println("post vals "+o.values.mkString(","))
    //val ser=o.values.asInstanceOf[Array[java.io.Serializable]]
    postStreamElement(ts,o.values)
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

class EmtCaller(who:PollWrapper,stationid:String) extends SystemCaller(who,stationid){ 
  class EmtGeo extends ServiceGEOSoap12Bindings with scalaxb.SoapClients with scalaxb.DispatchHttpClients{}
  lazy val emtGeo=new EmtGeo().service
  override def pollData={
    val data=emtGeo.getArriveStop(Some(who.user),
          Some(who.key),Some(stationid),Some(""),Some(""))
    if (data.isRight){
      //val ee=new es.emt.wsdl.GetArriveStopResult
      val res=data.right.get.getArriveStopResult.get.mixed.head.value.asInstanceOf[Elem]
      val date=new Date
      val dats=(res \ "Arrive").map{arr=>    
        val data:Array[java.io.Serializable]=Array(stationid, 
               (arr \ "idLine").head.text,
               (arr \ "TimeLeftBus").head.text.toInt,
               (arr \ "DistanceBus").head.text.toInt)
        new Observation(date,data)
      }
      dats
    }
    else null  
  }
}

class Observation(val timestamp:Date,val values:Array[java.io.Serializable])

abstract class SystemCaller(who:PollWrapper,systemid:String) extends Actor{
  //private lazy val client=Client.create
  //private lazy val webResource=client.resource(who.url+systemid+".json")
  private val df=new SimpleDateFormat(who.dateTimeFormat)//"yyyy-MM-dd HH:mm:ss.SSS")

  def pollData:Seq[Observation]
  
  def callRest{
    try{
      //val s= webResource.get(classOf[String])
      val obs=pollData//Array("1","3")//new Gson().fromJson(s,classOf[Array[BikeObservation]])
      obs.foreach{o=>
        //println(o.timestamp.dropRight(3))
        val datetime= try o.timestamp//try df.parse(o.timestamp.toString())
        catch {case e:Exception=>
          throw new IllegalArgumentException("Illegal data timestamp: "+o+".",e)}
        val c=Calendar.getInstance
        c.setTime(datetime)
        who.postData(systemid,c.getTimeInMillis,o)                           
      }
    } catch {case e:Exception=>e.printStackTrace}
  }
  
  context.setReceiveTimeout(who.rate millisecond) 
  def receive={
    //loop{      
      //reactWithin(who.rate){
        //case TIMEOUT=>callRest
         case ReceiveTimeout=>callRest
      //}
    //}
  }
}