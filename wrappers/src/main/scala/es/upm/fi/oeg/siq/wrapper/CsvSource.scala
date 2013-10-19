package es.upm.fi.oeg.siq.wrapper

import java.util.Date
import scala.io.Source

class CsvSource(who:PollWrapper,id:String) extends Datasource(who,id){      
  val idname=who.idkeys(id)
  val theurl=who.url.replace("{id}",id)
  lazy val rowrate=who.configvals("rowrate").toLong
  lazy val values=who.configvals("values").split(',').map{v=>new Func(v)}
  val funs = values.map{v=>v.instantiate}

  val data={
    val d=Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(theurl)).bufferedReader
    val dat=Stream.continually(d.readLine()).takeWhile(_!=null).map(extract(_))
    //d.close
    dat
  }
  override def afterPosting={
    if (rowrate>0) Thread.sleep(rowrate)
  }
  
  override def pollData={
    val date=new Date
    val res = data     
    res.map{data=>
      new Observation(date,Seq(idname)++data++funs.map(f=>f(id)))
    }       
  }
    
  def extract(string:String)={
    val array=string.split(';')
    var i=0
    array.map{value=>
      i+=1
      fieldTypes(i)(value)      
    }    
  }
}