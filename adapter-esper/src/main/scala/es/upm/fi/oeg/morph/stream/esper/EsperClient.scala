package es.upm.fi.oeg.morph.stream.esper

import com.typesafe.config.ConfigFactory
import akka.actor.{ ActorRef, Props, Actor, ActorSystem }
import akka.kernel.Bootable

object EsperClient extends App{
  val app = new LookupApplication
  app.doSomething(QueryMsg("mandioca!!!"))
  app.shutdown
  //println("beggggggin")
  //val system=ActorSystem("client")
  //connect
  //system.shutdown()
  /*
  def connect{
    println("connecting")
    val actor = system.actorFor("akka://hellokernel@127.0.0.1:2552/user/EsperServer")
    println("got it")
    actor ! QueryMsg("papalisa")
    println("after messaging")
  }
  */

}


class LookupApplication extends Bootable {
  //#setup
  val system = ActorSystem("LookupApplication", ConfigFactory.load.getConfig("remotelookup"))
  val actor = system.actorOf(Props[LookupActor], "lookupActor")
  val remoteActor = system.actorFor("akka://hellokernel@127.0.0.1:2552/user/EsperServer")

  def doSomething(op: QueryMsg) = {
    actor ! (remoteActor, op)
  }
  //#setup

  def startup() {
  }

  def shutdown() {
    system.shutdown()
  }
}


class LookupActor extends Actor {
  def receive = {
    case (actor: ActorRef, op: QueryMsg) ⇒ actor ! op
    
  }
}