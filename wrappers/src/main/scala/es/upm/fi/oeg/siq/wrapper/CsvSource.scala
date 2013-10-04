package es.upm.fi.oeg.siq.wrapper

import java.util.Date
import scala.io.Source

class CsvSource(who:PollWrapper,id:String) extends SystemCaller(who,id){      
  val idname=who.idkeys(id)
  val theurl=who.url.replace("{id}",id)
  val data={
    val d=Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(theurl)).bufferedReader
    val dat=Stream.continually(d.readLine()).takeWhile(_!=null).map(extract(_))
    //d.close
    dat
  }
  override def pollData={
    val date=new Date
    val res = data     
    res.map{data=>
      new Observation(date,Seq(idname)++data)
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