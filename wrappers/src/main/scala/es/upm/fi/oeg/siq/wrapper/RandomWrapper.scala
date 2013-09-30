package es.upm.fi.oeg.siq.wrapper

import akka.actor.Props
import java.util.Date
import scala.util.Random
import java.util.UUID
import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import java.util.Calendar

class RandomWrapper extends PollWrapper{
  
  lazy val gen=params.getPredicateValue("generator").split(',').map(_.toInt)
  lazy val values=params.getPredicateValue("values").split(',').map{v=>new Func(v)}
  override def initialize={
    setName("Random")
    setUsingRemoteTimestamp(true)
    true
  }
  override def getOutputFormat=dataFields 
  override def getWrapperName="RandomWrapper"
    
  override def run{
    systemids.foreach{systemid=>
      val actor = actorSystem.actorOf(Props(new SyntheticDatasource(this,systemid))) }
    while (isActive){
      Thread.sleep(liveRate)
      //println("still alive")
    }
  }

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
  
  class SyntheticDatasource(who:PollWrapper,id:String) extends SystemCaller(who,id){    
    val ids=(gen(0) until gen(1))//.map(i=>i.asInstanceOf[Integer])    
    val funs = values.map{v=>v.instantiate}
           
    override def pollData={
      val date=new Date
      ids.map(id=>new Observation(date,funs.map(f=>f(id))))            
    }    
  }

}

