package es.upm.fi.oeg.morph.esper
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

class EsperProxy(sys:ActorSystem,esperActorStr:String=null) {
  lazy val system = sys
  //val esperActorStr="akka://esperkernel@127.0.0.1:2552/user/EsperEngine" 
  private val esperPath=
    if (esperActorStr!=null) esperActorStr
    else "akka://esperkernel/user/EsperEngine"
  def engine=system.actorFor(esperPath)
  
}
