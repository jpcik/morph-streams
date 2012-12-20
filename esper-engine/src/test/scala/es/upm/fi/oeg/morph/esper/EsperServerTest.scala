package es.upm.fi.oeg.morph.esper
import org.scalatest.junit.ShouldMatchersForJUnit
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers
import org.junit.Before
import org.junit.Test
import org.junit.After
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._


class EsperServerTest  extends JUnitSuite with ShouldMatchersForJUnit with Checkers {
  lazy val esper=new EsperServer
    implicit val timeout = Timeout(5 seconds) // needed for `?` below
  lazy val proxy=new EsperProxy(esper.system)

    
  @Before def before{
    esper.startup 
    proxy.system.scheduler.schedule(0 seconds, 1 seconds){
    proxy.engine ! Event("wunderground",Map("temperature"->9.4,"stationId"->"ABT08"))}
  }
    
    
  @Test def startupTest{
    //val client = proxy.system.actorOf(Props(new EsperClient(proxy.engine)), "lookupActor")

    proxy.engine ! CreateWindow("wunderground","wund","")
        Thread.sleep(3000)

    val d=(proxy.engine ? ExecQuery("select * from wund"))
    //d.foreach(println)
    d onComplete {
      case Right(v)=>
        val list=v.asInstanceOf[Array[Object]]
        println("value "+list.mkString)
    }
    
    Thread.sleep(6000)
    println("wow")
    //client.shutdown
  }
  
  @After def shutdown{
    esper.shutdown
  }
}