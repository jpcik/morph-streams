package es.upm.fi.oeg.siq.wrapper

import dispatch._
import java.util.Date
import play.api.libs.json.Json._
import play.api.libs.json.Json
import play.api.libs.json.JsArray
import collection.JavaConversions._
import play.api.libs.json.JsString
import scala.xml.Elem
import com.ning.http.client.RequestBuilder


class RestApiSource (who:PollWrapper,id:String) extends Datasource(who,id){    
  val transf=who.datatype match{
    case "xml"=>svc:RequestBuilder=>extract(Http(svc OK as.xml.Elem)())
    case "json"=>svc:RequestBuilder=>extract(Http(svc OK as.String)())
  }
  val idname=who.idkeys(id)
  
  val theurl=who.url.replace("{id}",id)
  val svc = url(theurl)
  who.urlparams.foldLeft(svc){(a,k)=>
    if (k._2 == "{id}") svc.addQueryParameter(k._1, id)
    else svc.addQueryParameter(k._1, k._2)      
  }
  
  override def pollData={
    val date=new Date
    println(svc.url)
    val res = transf(svc)     
    res.map{data=>
      new Observation(date,Seq(idname)++data)
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
