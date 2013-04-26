/**
   Copyright 2010-2013 Ontology Engineering Group, Universidad Politécnica de Madrid, Spain

   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
   compliance with the License. You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software distributed under the License is 
   distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
   See the License for the specific language governing permissions and limitations under the License.
**/

package es.upm.fi.oeg.morph.stream.esper
import es.upm.fi.oeg.morph.stream.evaluate.StreamEvaluatorAdapter
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import java.util.Properties
import collection.JavaConversions._
import akka.actor.Props
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import concurrent.duration._
import es.upm.fi.oeg.morph.esper.ExecQuery
import es.upm.fi.oeg.morph.esper.RegisterQuery
import concurrent.Await
import es.upm.fi.oeg.morph.esper.PullData
import akka.actor.ActorSystem
import es.upm.fi.oeg.morph.esper.EsperProxy
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.esper.ListenQuery
import akka.actor.ActorRef
import akka.actor.Actor
import es.upm.fi.oeg.morph.stream.translate.DataTranslator
import es.upm.fi.oeg.morph.stream.evaluate.EvaluatorUtils
import scala.collection.mutable.ArrayBuffer
import es.upm.fi.oeg.morph.stream.evaluate.StreamReceiver
import org.slf4j.LoggerFactory
import scala.language.postfixOps

class EsperAdapter(props:Properties,system:ActorSystem) extends  StreamEvaluatorAdapter {
  lazy val proxy=new EsperProxy(system,props.getProperty("siq.adapter.esper.url"))
  implicit val timeout = Timeout(5 seconds) // needed for `?` below
  private val ids=new collection.mutable.HashMap[String,Seq[String]]
  
  def registerQuery(query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val qs:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unions
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
    val queries:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unions
      else Array(esperQuery) 
    val queryIds=ids.getOrElse(id,Seq(id)).zip(queries)
    val results=queryIds.map{qid=>
      val fut=(proxy.engine ? PullData(qid._1))
      val res=Await.result(fut,timeout.duration).asInstanceOf[Array[Array[Object]]]
      new EsperResultSet(res.toStream,qid._2.queryExpressions,qid._2.selectXprs.keys.toList.map(_.toString).toArray)
    }
    new EsperCompResultSet(results)
  }
    
  def listenQuery(query:SourceQuery,receiver:StreamReceiver){
    val esperQuery=query.asInstanceOf[EsperQuery]
    val queries:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unions
      else Array(esperQuery)
      
    queries.foreach{q=>
      val acrf=proxy.system.actorOf(Props(new StreamRec(receiver,q)),"reci"+System.currentTimeMillis)
      proxy.engine ! ListenQuery(q.serializeQuery,acrf)
    }
  }
  
  def executeQuery(query:SourceQuery) = {
    val esperQuery=query.asInstanceOf[EsperQuery]
    val id=registerQuery(query)
    Thread.sleep(3000)
    pull(id,query)         
  }
}

class StreamRec(rec:StreamReceiver,esperQuery:EsperQuery) extends Actor{
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val xprNames=esperQuery.selectXprs.keys.toList.map(_.toString).toArray
  private val varsX=esperQuery.queryExpressions
    
  def receive={
    case data:Array[Array[Object]]=>
        logger.trace("Array intercepted")
        val rs=new EsperResultSet(data.toStream,varsX,xprNames)
        val dt=new DataTranslator(List(rs),esperQuery)
        rec.receiveData(dt.transform)        
    case m=>logger.debug("got "+m)
        throw new IllegalArgumentException("Stream receiver got: "+m)
  }
}
