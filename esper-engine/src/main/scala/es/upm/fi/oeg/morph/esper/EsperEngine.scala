package es.upm.fi.oeg.morph.esper
import akka.actor.Actor
import com.espertech.esper.client.Configuration
import com.espertech.esper.client.EPServiceProviderManager
import collection.JavaConversions._
import com.espertech.esper.client.EPRuntime

class EsperEngine extends Actor{
  val configuration = new Configuration
  configuration.configure ("config/esper.xml")
  
  val epService = EPServiceProviderManager.getProvider("benchmark", configuration)
  val epAdministrator = epService.getEPAdministrator
  val epRuntime = epService.getEPRuntime

  //def stop=

  def receive={
    case Ping(msg)=>
      println(msg)
    case Event(name,attributes)=>
      //throw new Exception("papas")
	  println("data arrival "+attributes.mkString)
      epRuntime.sendEvent(attributes,name)
    case CreateWindow(name,window,duration)=>
      epAdministrator.createEPL("create window "+window+".win:keepall() as "+name)
      epAdministrator.createEPL("insert into "+window+" select * from "+name)
    case ExecQuery(query)=>      
      println("got this query "+query)
      val res=epRuntime.executeQuery(query)
      val pal=res.iterator.map{i=>
        i.get("temperature")}
      sender ! pal.toArray   	  
    case RegisterQuery(query)=>
      val ref=epAdministrator.createEPL(query)   	  
  	  sender ! ref.getName
	case PullData(id)=>
	  val r=epAdministrator.getStatement(id)
	  println("tibbi"+r.getAnnotations().mkString)
	  val propNames=r.getEventType.getPropertyNames
	  println("these are names: "+propNames.mkString)
	  val results=r.iterator.map{i=>
        propNames.map(key=>i.get(key)).toArray
      }
	  //r.iterator().foreach(a=>println("aaaaaa"+a))
	  
      sender ! results.toArray
	case _=> new Exception("papas")
  }
}