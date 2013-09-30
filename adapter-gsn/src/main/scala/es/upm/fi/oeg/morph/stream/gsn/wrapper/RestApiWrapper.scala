package es.upm.fi.oeg.morph.stream.gsn.wrapper
import gsn.wrappers.AbstractWrapper
import gsn.beans.DataField
import com.sun.jersey.api.client.Client
import com.google.gson.Gson
import java.util.Calendar
import gsn.beans.StreamElement
import java.text.SimpleDateFormat
import akka.actor.Actor
import concurrent.duration._
import akka.actor.ReceiveTimeout
import org.slf4j.LoggerFactory
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import scala.compat.Platform

class RestApiWrapper extends AbstractWrapper {
  private val actorSystem=ActorSystem("esperkernel",ConfigFactory.load.getConfig("restapiwrapper"))
  private val logger=LoggerFactory.getLogger(this.getClass)
  lazy val params=getActiveAddressBean  
  lazy val rate=params.getPredicateValue("rate").toLong
  lazy val liveRate=params.getPredicateValue("liverate").toLong
  lazy val dateTimeFormat=params.getPredicateValue("dateTimeFormat")
  lazy val url=params.getPredicateValue("url")
  lazy val systemids=params.getPredicateValue("systemids").split(',')
  lazy val systemnames=params.getPredicateValue("systemnames").split(',')
  private lazy val idnames=(systemids zip systemnames).toMap
  private lazy val fieldNames=params.getPredicateValue("fields").split(',')
  private lazy val types=params.getPredicateValue("types").split(',')
  private lazy val dataFields=fieldNames.zip(types).map(a=>new DataField(a._1,a._2,a._1))
      
  def postData(systemid:String,ts:Long,o:BikeObservation){
    logger.trace("data posted "+systemid+" "+o.id+" "+ts)
    //this.synchronized{          
    postStreamElement(ts,Array[java.io.Serializable]
     // postStreamElement(systemid,o.id,o.name,o.timestamp,o.bikes,o.free) 
      (idnames(systemid),o.id,o.name,o.timestamp,o.bikes,o.free))
    //}
    //println("data posted: "+systemid+"."+o)                       
  }
  
  override def initialize={
    setName("RestApiWrapper")
    setUsingRemoteTimestamp(true)
    true
  }
  override def getOutputFormat=dataFields
  override def dispose {}
  override def getWrapperName="RestApiWrapper"
    
  override def run{
    systemids.foreach{systemid=>
      val actor = actorSystem.actorOf(Props(new SystemCaller(this,systemid)))      
      //val sc=new SystemCaller(this,systemid)
      //sc.start
    }
    while (isActive){
      Thread.sleep(liveRate)
      logger.debug(getName+" Wrapper alive")
    }
  }
}

class SystemCaller(who:RestApiWrapper,systemid:String) extends Actor{
  private val logger=LoggerFactory.getLogger(this.getClass)
  private lazy val client=Client.create
  private lazy val webResource=client.resource(who.url+systemid+".json")
  private val df=new SimpleDateFormat(who.dateTimeFormat)//"yyyy-MM-dd HH:mm:ss.SSS")

  def callRest{
    try{
      val s= webResource.get(classOf[String])
      logger.trace("Retrieving data: "+webResource)
      val obs=new Gson().fromJson(s,classOf[Array[BikeObservation]])
      obs.foreach{o=>
        //println(o.timestamp.dropRight(3))
        val datetime= try df.parse(o.timestamp.dropRight(3))
        catch {case e:Exception=>
          throw new IllegalArgumentException("Illegal data timestamp: "+o.timestamp+".",e)}
        val c=Calendar.getInstance
        c.setTime(datetime)
        who.postData(systemid,c.getTimeInMillis,o)                           
      }
    } catch {case e:Exception=>e.printStackTrace}
  }
  val tiemout=(who.rate millisecond)
  context.setReceiveTimeout(tiemout) 
  def receive={
      case ReceiveTimeout=>callRest        
  }
}