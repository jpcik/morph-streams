package es.upm.fi.oeg.siq.wrapper

import java.text.SimpleDateFormat
import java.util.Calendar
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ReceiveTimeout
import concurrent.duration._
import dispatch._
import collection.JavaConversions._
import org.joda.time.DateTime
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import es.upm.fi.oeg.morph.esper.Event

class ApiWrapper(systemid:String) extends Actor{
  val conf=ConfigFactory.load getConfig(systemid)
  val ids=conf.getStringList("ids")
  ids.foreach(id=>context.actorOf(Props(new ApiCaller(systemid,id,conf)), "wrapper-"+systemid+"-"+id))
  def receive={
    case _=>
  }
}

class ApiCaller (systemid:String,id:String,conf:Config) extends Actor{
  val rate=conf.getLong("rate")
  val fields:Seq[Field]={
    val apifields=conf.getStringList("api.data")
    val outfields=conf.getStringList("outputfields")
    val outtypes=conf.getStringList("outputtypes")
    val pattern = """\{.*""".r

    ((apifields zip outfields) zip outtypes).map(a=>a._1._1 match {
      case pattern()=>
        ConstField(a._1._2,a._2,constants(a._1._1))
      case _=>VarField(a._1._2,a._2,a._1._1)
    })
  }.toSeq

  val iterable=conf.getString("api.iterable")
  val outputstream=conf.getString("outputstream")
  val theurl=conf.getString("api.url")
  val paramNames=conf.getStringList("api.paramNames")
  val params={
    val pars=conf.getStringList("api.params").map(p=>constants(p))    
    (paramNames zip pars).toMap
  }
  val engine=this.context.actorFor(conf.getString("engineurl"))
  private val df=new SimpleDateFormat(conf.getString("dateTimeFormat"))
  private def constants(op:String)=op match{
      case "{id}"=>id
      case _=>op   
  }
  
  
  def pollData={
    val svc = url(theurl)
    params.foldLeft(svc)((a,k)=>svc.addQueryParameter(k._1, k._2))
    val xml = Http(svc OK as.xml.Elem)     
    val data=(xml() \\ iterable).map{iter=>
      fields.map{f=>
        f.extract(iter)
      }.toMap
    }
    data
  }
  
  
  def callRest{
    try{    
      val obs=pollData
      obs.foreach{o=>
        engine ! Event(outputstream,o)   
        /*val datetime= try o.timestamp//try df.parse(o.timestamp.toString())
        val c=Calendar.getInstance
        c.setTime(datetime)*/
      }
    } catch {case e:Exception=>e.printStackTrace}
  }
  
  context.setReceiveTimeout(rate millisecond) 
  def receive={
    case ReceiveTimeout=>callRest
  }

  implicit class Typed(value:String){
    def typing(typeString:String)=typeString match{
      case "int"=>value.toInt
      case "double"=>value.toDouble
      case "long"=>value.toLong
      case _=>value  
    }
  }

  abstract class Field(name:String,typeStr:String){
    def extract(node:xml.Node):(String,Any)
  }
  case class VarField(name:String,typeStr:String,inName:String) extends Field(name,typeStr){
        def extract(node:xml.Node)={
           name -> ((node \ inName) text).typing(typeStr)          
        }

  }
  case class ConstField(name:String,typeStr:String,value:Any) extends Field(name,typeStr){
        def extract(node:xml.Node)=(name,value)

  }
}