package es.upm.fi.oeg.morph.wrapper.gsn

import gsn.wrappers.AbstractWrapper
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.siq.wrapper.Observation
import gsn.beans.DataField
import es.upm.fi.oeg.siq.wrapper.PollWrapper
import akka.actor.Props
import es.upm.fi.oeg.siq.wrapper.RestApiSource
import es.upm.fi.oeg.siq.wrapper.SyntheticDatasource
import collection.JavaConversions._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

class GsnPollWrapper extends AbstractWrapper with PollWrapper {
  override val logger=LoggerFactory.getLogger(this.getClass)
  lazy val params=getActiveAddressBean
  override def configvals(key:String)= params.getPredicateValue(key)
  override val actorSystem=ActorSystem("wrap",ConfigFactory.load.getConfig("restapiwrapper"))
  
  override def postData(systemid:String,ts:Long,o:Observation){
    logger.trace("post vals "+o.serializable.size)    
    //val ser =o.values.asInstanceOf[Array[java.io.Serializable]]
    postStreamElement(ts,o.serializable)
    //postStreamElement(new StreamElement(dataFields,o.serializable),ts)    
    //Array[java.io.Serializable](systemid,o.id,o.name,o.timestamp,o.bikes,o.free))              
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
    systemids.foreach{systemid=>datasourcetype match{
      case "restapi"=> val actor = actorSystem.actorOf(Props(new RestApiSource(this,systemid)))
      case "random"=> val actor = actorSystem.actorOf(Props(new SyntheticDatasource(this,systemid)))
    }}
    while (isActive){
      Thread.sleep(liveRate)
      println("still alive")
    }
  }
}