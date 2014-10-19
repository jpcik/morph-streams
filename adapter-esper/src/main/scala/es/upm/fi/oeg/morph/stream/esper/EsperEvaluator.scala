package es.upm.fi.oeg.morph.stream.esper

import es.upm.fi.oeg.morph.stream.evaluate.AsyncEvaluator
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import scala.concurrent.Await
import akka.util.Timeout
import akka.pattern.ask
import concurrent.duration._
import language.postfixOps
import com.typesafe.config.ConfigFactory
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.esper.RegisterQuery
import es.upm.fi.oeg.morph.stream.evaluate.ComposedResultSet
import es.upm.fi.oeg.morph.esper.PullData
import akka.actor.ActorRef
import es.upm.fi.oeg.morph.esper.ListenQuery
import es.upm.fi.oeg.morph.stream.evaluate.StreamReceiver

class EsperEvaluator(systemid:String="esper") extends AsyncEvaluator(systemid) {
  val config = ConfigFactory.load.getConfig("morph.streams.adapter."+systemid)
  lazy val proxy=new EsperProxy(context.system,config.getString("url"))
  implicit val timeout = Timeout(5 seconds) // needed for `?` below
  private val ids=new collection.mutable.HashMap[String,Seq[String]]

  def specificMessageHandler: Receive = {
    case s:String => s.toString
  }
      
  override def i_registerQuery(query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val qs:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unionQueries
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

  
  override def i_pull(id:String,query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val queries:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unionQueries
      else Array(esperQuery) 
    val queryIds=ids.getOrElse(id,Seq(id)).zip(queries)
    val results=queryIds.map{qid=>
      val fut=(proxy.engine ? PullData(qid._1))
      val res=Await.result(fut,timeout.duration).asInstanceOf[Array[Array[Object]]]
      new EsperResultSet(res.toStream,qid._2.queryExpressions,qid._2.selectXprs.keys.toList.map(_.toString).toSeq)
    }
    new ComposedResultSet(results)
  }
    
  override def i_listenToQuery(query:SourceQuery,receiver:StreamReceiver)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val queries:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unionQueries
      else Array(esperQuery)
      
    queries.foreach{q=>
      //val acrf=proxy.system.actorOf(Props(new StreamRec(receiver,q)),"reci"+System.nanoTime)
      proxy.engine ! ListenQuery(q.serializeQuery,null)
    }
    null
  }
  
  override def i_executeQuery(query:SourceQuery) = {
    val esperQuery=query.asInstanceOf[EsperQuery]
    val id=i_registerQuery(query)
    Thread.sleep(3000)
    i_pull(id,query)         
  }

}