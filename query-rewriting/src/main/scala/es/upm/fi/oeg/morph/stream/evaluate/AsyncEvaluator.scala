package es.upm.fi.oeg.morph.stream.evaluate

import java.net.URI
import akka.actor.Actor

abstract class AsyncEvaluator(systemId:String) extends QueryEvaluator(systemId) 
  with Actor{
      
 def specificMessageHandler: Receive
 
 
  def receive = specificMessageHandler orElse genericMessageHandler
  def genericMessageHandler: Receive = {
    case ExecuteQuery(sparqlstr,mappingUri)=>
      sender ! Data(executeQuery(sparqlstr,mappingUri))
    case RegisterQuery(sparqlstr,mappingUri)=>
      sender ! QueryId(registerQuery(sparqlstr, mappingUri))
    case PullData(id)=>
      sender ! Data(pull(id))
    case ListenToQuery(sparqlstr,mappingUri)=>
      //listenToQuery(sparqlstr, mappingUri,sender)
  }

}

case class RegisterQuery(sparqlquery:String,mappingUri:URI)
case class ListenToQuery(sparqlquery:String,mappingUri:URI)
case class ExecuteQuery(sparqlquery:String,mappingUri:URI)
case class PullData(id:String)
case class Data(results:Object)
case class QueryId(id:String)
