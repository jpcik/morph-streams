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
import com.typesafe.config.ConfigFactory
import es.upm.fi.oeg.morph.stream.evaluate.ComposedResultSet
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import es.upm.fi.oeg.morph.stream.evaluate.DataReceiver
import es.upm.fi.oeg.morph.stream.evaluate.StreamReceiver
import es.upm.fi.oeg.morph.stream.evaluate.ExecuteQuery
import es.upm.fi.oeg.morph.esper.RemoveQuery

class EsperAdapter(sys:ActorSystem,systemId:String="esper") extends QueryEvaluator(systemId) {
  val config = ConfigFactory.load.getConfig("morph.streams."+systemId+".adapter")
  lazy val proxy=new EsperProxy(sys,config.getString("url"))
  implicit val timeout = Timeout(5 seconds) // needed for `?` below
  private val ids=new collection.mutable.HashMap[String,Seq[String]]
    
  import concurrent.ExecutionContext.Implicits.global 
  override def i_registerQuery(query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val qs:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unionQueries
      else Array(esperQuery)
    val queryIds=qs.map{q =>    	
      val d=(proxy.engine ? RegisterQuery(q.serializeQuery)).recover{
        case e:Exception => throw e}
      val id=Await.result(d,timeout.duration).asInstanceOf[String]      
      id
    }
    getQueryId(queryIds)
  }

  //Not the best solution, for multi queries
  private def getQueryId(queryIds:Seq[String])={
    if (queryIds.size>1)
      ids.+=((queryIds.head,queryIds))
    queryIds.head
  }
  
  override def i_rewriteSerialize(query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val qs:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unionQueries
      else Array(esperQuery)
    Some(qs.map(q=>q.serializeQuery).mkString(" UNION\n "))
  }
  
  override def i_pull(id:String,query:SourceQuery)={
    val esperQuery=query.asInstanceOf[EsperQuery]
    val queries:Seq[EsperQuery]=
      if (esperQuery.unions.size>0) esperQuery.unionQueries
      else Array(esperQuery) 
    val queryIds=ids.getOrElse(id,Seq(id)).zip(queries)
    val results=queryIds.map{qid=>
      val fut=(proxy.engine ? PullData(qid._1)).recover{
        case e:Exception => throw e}
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
      
    val queryIds=queries.map{q=>
      val acrf=proxy.system.actorOf(Props(new StreamRec(receiver,q)),"reci"+System.nanoTime)
      val d=(proxy.engine ? ListenQuery(q.serializeQuery,acrf)).recover{
        case e:Exception => throw e}
      Await.result(d,timeout.duration).asInstanceOf[String]            
    }
    getQueryId(queryIds)
  }
  
  override def i_executeQuery(query:SourceQuery) = {
    val esperQuery=query.asInstanceOf[EsperQuery]
    val d=(proxy.engine ? ExecQuery(esperQuery.serializeQuery))
    val res= Await.result(d,timeout.duration).asInstanceOf[Array[Array[Object]]]
    new EsperResultSet(res.toStream,esperQuery.queryExpressions,
        esperQuery.selectXprs.keys.toList.map(_.toString).toSeq)
  }
  
  override def i_removeQuery(id:String)={
    proxy.engine ? RemoveQuery(id)        
  }
}

class StreamRec(rec:StreamReceiver,esperQuery:EsperQuery) extends DataReceiver(rec,esperQuery){
  override def resultSet(data:Stream[Array[Object]],query:SourceQuery)={
    val xprNames=esperQuery.selectXprs.keys.toList.map(_.toString).toArray
    val varsX=esperQuery.queryExpressions
    new EsperResultSet(data.toStream,varsX,xprNames)
  }

}

