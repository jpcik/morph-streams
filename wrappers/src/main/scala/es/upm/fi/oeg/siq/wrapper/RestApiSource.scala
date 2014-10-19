package es.upm.fi.oeg.siq.wrapper

import dispatch._, Defaults._
import java.util.Date
import play.api.libs.json.Json._
import play.api.libs.json.Json
import play.api.libs.json.JsArray
import collection.JavaConversions._
import play.api.libs.json.JsString
import scala.xml.Elem
//import com.ning.http.client.RequestBuilder
import play.api.libs.json.JsValue
import org.slf4j.LoggerFactory


class RestApiSource (who:PollWrapper,id:String) extends Datasource(who,id){
  val logger = LoggerFactory.getLogger(this.getClass)
  val theurl=who.url.replace("{id}",id)
  val svc = url(theurl)
  val transf=who.datatype match{
    case "xml"=>svc:dispatch.Req=>extract(Http(svc OK as.xml.Elem).apply)
    case "json"=>
      println(theurl)
      val str=Http(svc OK as.String).apply
      println(str)
      svc:dispatch.Req=>extract(str)
  }
  val idname=who.idkeys(id)
  
  who.urlparams.foldLeft(svc){(a,k)=>
    if (k._2 == "{id}") svc.addQueryParameter(k._1, id)
    else svc.addQueryParameter(k._1, k._2)      
  }
  if (logger.isTraceEnabled)
    logger.trace("Service to call: "+svc.url)
  
  val root=who.configvals("serviceroot")
  lazy val values=who.configvals("values").split(',').filterNot(_=="").map{v=>new Func(v)}
  val funs = values.map{v=>v.instantiate}

  override def pollData={
    val date=new Date
    //println(svc.url)
    val res = transf(svc)     
    res.map{data=>
      new Observation(date,Seq(idname)++data++funs.map(f=>f(id)))
    }            
  }
    
  def extract(xml:Elem) ={
    xml.child.filter(_.isInstanceOf[Elem]).map{e=>
      var i=0
      who.servicefields.map{key=>
        val str= (e \ key).head
        i+=1
        fieldTypes(i)(str.text)         
      }
    } 
  }
    
  def extract(string:String)={
    val json=Json.parse(string)
    val jsonlist=if (root!="") (json \ root).as[JsArray] else json.as[JsArray]
    
    jsonlist.value.map{js=>
      var i=0
      who.servicefields.map{key=>
        val str=navigate(js,key) match{
          case st:JsString=>st.value
          case p =>p.toString   
        }        
        i+=1
        fieldTypes(i)(str)
      }
    }    
  }
  
  private def navigate(json:JsValue,path:String)={
    path.split("/").foldLeft(json)((jsval,subpath)=>jsval \ subpath)    
  }
}
