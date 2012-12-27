package es.upm.fi.oeg.morph.stream.esper
import es.upm.fi.oeg.morph.stream.evaluate.StreamEvaluatorAdapter
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import java.util.Properties
import scala.actors.Future
import collection.JavaConversions._
import akka.actor.Props
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import akka.util.duration._
import es.upm.fi.oeg.morph.esper.ExecQuery
import es.upm.fi.oeg.morph.esper.RegisterQuery
import akka.dispatch.Await
import es.upm.fi.oeg.morph.esper.PullData
import akka.actor.ActorSystem
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.stream.evaluate.StreamReceiver
import es.upm.fi.oeg.morph.esper.ListenQuery
import akka.actor.ActorRef
import akka.actor.Actor
import es.upm.fi.oeg.morph.stream.translate.DataTranslator
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils

class EsperAdapter(props:Properties,system:ActorSystem) extends  StreamEvaluatorAdapter {
  lazy val proxy=new EsperProxy(system,props.getProperty("siq.adapter.esper.url"))
  implicit val timeout = Timeout(5 seconds) // needed for `?` below
  
  def registerQuery(query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val d=(proxy.engine ? RegisterQuery(query.serializeQuery))
    val id= Await.result(d,timeout.duration).asInstanceOf[String]
    id
  }
  
  def pull(id:String,query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val fut=(proxy.engine ? PullData(id))
    val res=Await.result(fut,timeout.duration).asInstanceOf[Array[Array[Object]]]
    println("tatata"+esperQuery.selectXprs)
    new EsperResultSet(res.toStream,esperQuery.varsX,esperQuery.selectXprs.values.map(_.toString).toArray)
  }
  
  class StreamRec(rec:StreamReceiver,esperQuery:EsperQuery) extends Actor{
    def receive={
      case data:Array[Array[Object]]=>println("array intercepted")
        val rs=new EsperResultSet(data.toStream,esperQuery.varsX,esperQuery.selectXprs.values.map(_.toString).toArray)
        val dt=new DataTranslator(List(rs),esperQuery)
        val sparql=dt.transform     
        rec.receiveData(EvaluatorUtils.sparqlString(sparql))
      case m=>println("got "+m)
        rec.receiveData(m.toString)
    }
  }
  
  def listenQuery(query:SourceQuery,receiver:StreamReceiver){
    val esperQuery=query.asInstanceOf[EsperQuery]
    println("sending query now")
    val acrf=proxy.system.actorOf(Props(new StreamRec(receiver,esperQuery)),"reci")

    proxy.engine ! ListenQuery(query.serializeQuery,acrf)
    
  }
  
  def executeQuery(query:SourceQuery) = {
    val esperQuery=query.asInstanceOf[EsperQuery]
    val id=registerQuery(query)
    Thread.sleep(3000)
    pull(id,query)
    
    val map=query.getProjection.map(p=>(p._1,VarXpr(p._2)))
    val data=Stream(Array("r1","4","5"),Array("r2","4","3"))
    
    new EsperResultSet(data.asInstanceOf[Stream[Array[Object]]],esperQuery.projectionXprs,null) 
  }
}

