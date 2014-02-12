package es.upm.fi.oeg.siq.wrapper

import akka.actor.Actor
import scala.compat.Platform
import akka.actor.ReceiveTimeout
import scala.concurrent.duration._
import scala.util.Random
import java.util.UUID
import java.text.SimpleDateFormat
import java.util.Calendar

abstract class Datasource(who:PollWrapper,systemid:String) extends Actor{
  def pollData:Seq[Observation]
  def afterPosting:Unit={}
  
  def callRest{
    try{  
      val obs=pollData
      val start=Platform.currentTime
      var i=0
      obs.foreach{o=>        
        //val datetime= o.timestamp
        //val c=Calendar.getInstance
        //c.setTime(datetime)
        who.postData(systemid,start+i,o)
        afterPosting
        i+=1
      }
    } catch {case e:Exception=>e.printStackTrace}
  }
  
  override def preStart(){
    callRest
  }
  
  context.setReceiveTimeout(who.rate millisecond) 
  def receive={
    case ReceiveTimeout=>callRest
  }
  
  protected val fieldTypes=who.dataFields.map{ _.getType match{
    case "int"=>vl:String=>vl.toInt
    case "long"=>vl:String=>vl.toLong
    case "double"=>vl:String=>vl.toDouble
    case _=>vl:String=>vl  
  }}

  case class Func(name:String,values:String){
    def this(v:String)=this(v.substring(0,v.indexOf("(")),
           v.substring(v.indexOf("(")+1).replace(")", ""))
    
    def instantiate:Any=>java.io.Serializable= this.name match {
      case "random"=>
        val rnd=this.values.split("\\+").map(_.toInt)
        par:Any=>Random.nextInt(rnd(0))+rnd(1)
      case "uuid"=>par:Any=>UUID.randomUUID.toString
      case "format"=>par:Any=>this.values.format(par)
      case "dateformat"=>
        val dtFormat=new SimpleDateFormat(this.values)
        par:Any=>dtFormat.format(Calendar.getInstance.getTime)
    }
  }
}