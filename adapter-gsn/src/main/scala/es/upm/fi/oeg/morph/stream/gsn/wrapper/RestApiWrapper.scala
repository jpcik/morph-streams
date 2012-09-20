package es.upm.fi.oeg.morph.stream.gsn.wrapper
import gsn.wrappers.AbstractWrapper
import gsn.beans.DataField
import com.sun.jersey.api.client.Client
import com.google.gson.Gson
import java.util.Calendar
import scala.actors.Actor
import scala.actors._
import scala.actors.Actor._
import gsn.beans.StreamElement
import java.text.SimpleDateFormat

class RestApiWrapper extends AbstractWrapper {
  
  lazy val params=getActiveAddressBean
  lazy val rate=params.getPredicateValue("rate").toLong
  lazy val liveRate=rate
  lazy val dateTimeFormat=params.getPredicateValue("dateTimeFormat")
  lazy val url=params.getPredicateValue("url")
  lazy val systemids=params.getPredicateValue("systemids").split(',')
  private lazy val fieldNames=params.getPredicateValue("fields").split(',')
  private lazy val types=params.getPredicateValue("types").split(',')
  private lazy val dataFields=fieldNames.zip(types).map(a=>new DataField(a._1,a._2,a._1))
      
  def postData(systemid:String,ts:Long,o:BikeObservation){
    postStreamElement(ts,Array[java.io.Serializable]
                         (systemid,o.id,o.name,o.timestamp,o.bikes,o.free))
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
      val sc=new SystemCaller(this,systemid)
      sc.start
    }
    while (isActive){
      Thread.sleep(liveRate)
      println("still alive")
    }
  }
}

class SystemCaller(who:RestApiWrapper,systemid:String) extends Actor{
  private lazy val client=Client.create
  private lazy val webResource=client.resource(who.url+systemid+".json")
  private val df=new SimpleDateFormat(who.dateTimeFormat)//"yyyy-MM-dd HH:mm:ss.SSS")

  def callRest{
    try{
      val s= webResource.get(classOf[String])
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
  
  def act(){
    loop{      
    reactWithin(who.rate){
      case TIMEOUT=>callRest
    }
    }
  }
}