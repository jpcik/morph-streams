package es.upm.fi.oeg.morph.streams.esper
import com.weiglewilczek.slf4s.Logging
//import akka.actor._
//import akka.routing.RoundRobinRouter
import scala.actors.Actor
import collection.JavaConversions._
import com.espertech.esper.client.Configuration
import com.espertech.esper.client.EPServiceProviderManager
import java.util.Random
import com.espertech.esper.client.EPRuntime
import com.google.common.collect.Maps


class EsperEngine extends Actor with Logging{
  val configuration = new Configuration
  configuration.configure ("config/esper.xml")
  
  val epService = EPServiceProviderManager.getProvider("benchmark", configuration)
  val epAdministrator = epService.getEPAdministrator
  val epRuntime = epService.getEPRuntime

  
  def act(){
    while (true){
      receive {
        case RegisteredStream(name,expiry)=>
          epAdministrator.createEPL("create window "+name+"winn.win:keepall() as select * from "+name)
          epAdministrator.createEPL("insert into "+name+"winn select * from "+name)
    	case QueryMsg(query)=>
    	  logger.debug("received: "+query)
    	  logger.debug("afterwards")
    	  val res=epRuntime.executeQuery(query)
    	  res.getArray.foreach(e=>println(e.getEventType().getPropertyNames().map(p=>e.get(p)).mkString(",")))
    	case Event(name,attributes)=>
    	  epRuntime.sendEvent(attributes,name)
      } 
    	  
   }                                            
  }    

}

class RandomStreamer(extent:String,rate:Int,esper:EsperEngine) 
  extends EsperStreamer(extent,rate,esper){
  val r=new Random		
  override def generateData={
	val time = r.nextLong
	val map=List(("DateTime", r.nextLong),("Hs", r.nextDouble),("timestamp", time)).toMap
	Some(map)
  }
  
}

abstract class EsperStreamer(extent:String,rate:Int,esper:EsperEngine) extends Actor with Logging{
  def generateData:Option[Map[String,_]]
  def act(){
	while (true) {
	  val data=generateData
	  if (data.isDefined){
	    logger.debug("sending ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")	  
		esper ! Event(extent,data.get)
	  }
	  Thread.sleep(rate*1000);
    }
  }
}

case class Event(name:String,attributes:Map[String,Any])
case class RegisteredStream(name:String,expiry:Int)
case class QueryMsg(query:String)
