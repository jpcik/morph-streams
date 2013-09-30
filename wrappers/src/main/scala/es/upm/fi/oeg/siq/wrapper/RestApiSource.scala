package es.upm.fi.oeg.siq.wrapper

import dispatch._
import java.util.Date
import play.api.libs.json.Json._
import play.api.libs.json.Json
import play.api.libs.json.JsArray
import collection.JavaConversions._
import play.api.libs.json.JsString
import akka.actor.Props
import scala.xml.Elem
import com.ning.http.client.RequestBuilder



class RestApiSource (who:PollWrapper,id:String) extends SystemCaller(who,id){    
    //val funs = values.map{v=>v.instantiate}
  val transf=who.datatype match{
    case "xml"=>svc:RequestBuilder=>extract(Http(svc OK as.xml.Elem)())
    case "json"=>svc:RequestBuilder=>extract(Http(svc OK as.String)())
  }
  val idname=who.idkeys(id)
    val fieldTypes=who.dataFields.map{ _.getType match{
      case "int"=>vl:String=>vl.toInt
      case _=>vl:String=>vl
    }}
    val theurl=who.url.replace("{id}",id)
    val svc = url(theurl)
    who.urlparams.foldLeft(svc){(a,k)=>
      if (k._2 == "{id}") svc.addQueryParameter(k._1, id)
      else svc.addQueryParameter(k._1, k._2)      
    }
    override def pollData={
      val date=new Date
                      
      val res = transf(svc)     

      res.map{data=>
        new Observation(date,Seq(idname)++data)
      }

            
    }
    
   def extract(xml:Elem) ={
     xml.child.map{e=>
      var i=0
      who.servicefields.map{key=>
        val str= (e \ key).head
        i+=1
        fieldTypes(i)(str.text)         
       }
     } 
   }
    
   def extract(string:String)={
    val json=Json.parse(string).as[JsArray]
    json.value.map{js=>
      var i=0
      who.servicefields.map{key=>
        val str=(js \ key) match{
          case st:JsString=>st.value
          case p =>p.toString          
        }        
        i+=1
        fieldTypes(i)(str)
      }
    }
    
  }
}


class RestApiPollWrapper extends PollWrapper{
  
  override def initialize={
    setName("RestApiPoll")
    setUsingRemoteTimestamp(true)
    true
  }
  override def getOutputFormat=dataFields 
  override def getWrapperName="RestApiPoll"
    
  override def run{
    systemids.foreach{systemid=>
      val actor = actorSystem.actorOf(Props(new RestApiSource(this,systemid))) }
    while (isActive){
      Thread.sleep(liveRate)
    }
  }
}