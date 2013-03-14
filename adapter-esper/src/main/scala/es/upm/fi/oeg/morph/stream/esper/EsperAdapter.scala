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
import scala.collection.mutable.ArrayBuffer

class EsperAdapter(props:Properties,system:ActorSystem) extends  StreamEvaluatorAdapter {
  lazy val proxy=new EsperProxy(system,props.getProperty("siq.adapter.esper.url"))
  implicit val timeout = Timeout(5 seconds) // needed for `?` below
  private val ids=new collection.mutable.HashMap[String,Seq[String]]
  
  def registerQuery(query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val qs:Seq[EsperQuery]=if (esperQuery.unions.size>0)
    	esperQuery.unions
    	else Array(esperQuery)
    val queryIds=qs.map{q =>    	
      val d=(proxy.engine ? RegisterQuery(q.serializeQuery))
      val id= Await.result(d,timeout.duration).asInstanceOf[String]
      id
    }
    if (queryIds.size>1)
      ids.+=((queryIds.head,queryIds))
    queryIds.head
  }
  
  def pull(id:String,query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val queries:Seq[EsperQuery]=if (esperQuery.unions.size>0) esperQuery.unions
      else Array(esperQuery) 
    val queryIds=ids.getOrElse(id,Seq(id)).zip(queries)
    val results=queryIds.map{qid=>
      val fut=(proxy.engine ? PullData(qid._1))
      val res=Await.result(fut,timeout.duration).asInstanceOf[Array[Array[Object]]]
      println("tatata"+qid._2.allXprs)
      println("mm"+qid._2.allXprs.keys.toList.map(_.toString).toArray.mkString)
      new EsperResultSet(res.toStream,qid._2.varsX,qid._2.allXprs.keys.toList.map(_.toString).toArray)
    }
    new EsperCompResultSet(results)
  }
  
  class StreamRec(rec:StreamReceiver,esperQuery:EsperQuery) extends Actor{
    def receive={
      case data:Array[Array[Object]]=>println("array intercepted")
        val rs=new EsperResultSet(data.toStream,esperQuery.varsX,esperQuery.selectXprs.values.map(_.toString).toArray)
        val dt=new DataTranslator(List(rs),esperQuery)
        val sparql=dt.transform     
        rec.receiveData(sparql)
      case m=>println("got "+m)
        rec.receiveData(null)
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

