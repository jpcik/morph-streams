package es.upm.fi.oeg.siq.wrapper

import java.util.Date

class SyntheticDatasource(who:PollWrapper,id:String) extends Datasource(who,id){     
  lazy val gen=who.configvals("generator").split(',').map(_.toInt)
  lazy val values=who.configvals("values").split(',').map{v=>new Func(v)}
 
  val ids=(gen(0) until gen(1))    
  val funs = values.map{v=>v.instantiate}
           
  override def pollData={
    ids.map(id=>new Observation(new Date,funs.map(f=>f(id))))            
  }    
}

