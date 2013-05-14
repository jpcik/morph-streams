package es.upm.fi.oeg.morph.esper
import akka.actor.Actor
import com.espertech.esper.client.Configuration
import com.espertech.esper.client.EPServiceProviderManager
import collection.JavaConversions._
import com.espertech.esper.client.EPRuntime
import com.espertech.esper.client.UpdateListener
import com.espertech.esper.client.EventBean
import org.slf4j.LoggerFactory

class EsperEngine extends Actor{
  private val configuration = new Configuration
  configuration.configure ("config/esper.xml")
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val epService = EPServiceProviderManager.getProvider("benchmark", configuration)
  private val epAdministrator = epService.getEPAdministrator
  private val epRuntime = epService.getEPRuntime
  
  def receive={
    case Ping(msg)=>logger.info("Received ping: "+msg)
    case Event(name,attributes)=>epRuntime.sendEvent(attributes,name)
    case CreateWindow(name,window,duration)=>
      epAdministrator.createEPL("create window "+window+".win:keepall() as "+name)
      epAdministrator.createEPL("insert into "+window+" select * from "+name)
    case ExecQuery(query)=>      
      logger.debug("Recieved this query to exec: "+query)
      val res=epRuntime.executeQuery(query)
	  val propNames=res.getEventType.getPropertyNames
	  val results=res.iterator.map{i=>
        propNames.map(key=>i.get(key)).toArray
      }
      sender ! results.toArray   	  
    case RegisterQuery(query)=>
      logger.debug("Recieved this query to register: "+query)
      val ref=epAdministrator.createEPL(query)
  	  sender ! ref.getName
    case ListenQuery(query,actor)=>
      logger.debug("Recieved this query to listen: "+query)            
      val ref=epAdministrator.createEPL(query)
   	  val propNames=ref.getEventType.getPropertyNames
      ref.addListener(new UpdateListener{
        def update(newData:Array[EventBean],oldData:Array[EventBean]){  
                      
          /*if (oldData!=null){  
            logger.debug("Sending old events: "+oldData.map(i=>propNames.map(i.get(_)).mkString(",")).mkString)
            actor ! oldData.map{i=>propNames.map(key=>i.get(key))}
          }*/
          if (newData!=null){
            if (logger.isTraceEnabled)
              logger.trace("Sending events: "+newData.map(i=>propNames.map(i.get(_)).mkString(",")).mkString)
            actor ! newData.map{i=>propNames.map(key=>i.get(key))}
          }
        }
      })
  	  sender ! ref.getName
	case PullData(id)=>
      logger.debug("Recieved pull data: "+id)            
	  val r=epAdministrator.getStatement(id)
	  val propNames=r.getEventType.getPropertyNames
	  val results=r.iterator.map{i=>
        propNames.map(key=>i.get(key)).toArray
      }	  
      sender ! results.toArray
	case msg=> new Exception("Unknown message arrived: "+msg)
  }
}