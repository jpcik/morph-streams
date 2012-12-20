package es.upm.fi.oeg.morph.esper
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._
import akka.actor.ActorRef
import akka.dispatch.Await


class EsperClient(engine:ActorRef) extends Actor{
  
  implicit val timeout = Timeout(5 seconds) // needed for `?` below
  
  def query(q:ExecQuery){
    engine ? q
  }
  
   def receive={
     case e:Event=>
       engine ! e
     case q:ExecQuery=>
        val d= (engine ? q).mapTo[String]
        d onComplete {
          case a=>println("value "+a)
        }
        //val res=Await.result(d,1 second)
        //println("value "+res)
     case m=> println("got this: "+m)
   }

}