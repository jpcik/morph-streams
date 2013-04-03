package es.upm.fi.oeg.morph.stream.evaluate
import java.net.URI
import es.upm.fi.oeg.sparqlstream.SparqlStream
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import java.sql.ResultSet
import java.util.Properties
import es.upm.fi.oeg.morph.stream.translate.DataTranslator
import javax.xml.bind.JAXBContext
import java.io.StringWriter
import javax.xml.bind.Marshaller
import javax.xml.bind.JAXBException
import es.upm.fi.oeg.morph.voc.RDFFormat
import akka.actor.Actor
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.ActorRef
import akka.actor.Actor._
import akka.actor.Props
import es.upm.fi.oeg.siq.tools.ParameterUtils
import es.upm.fi.oeg.siq.sparql.SparqlResults
import com.hp.hpl.jena.query.ResultSetFormatter
import org.slf4j.LoggerFactory

trait StreamEvaluatorAdapter {
  def executeQuery(abstractQuery:SourceQuery):ResultSet
  def registerQuery(abstractQuery:SourceQuery):String
  def pull(id:String,query:SourceQuery):ResultSet
  def listenQuery(abstractQuery:SourceQuery,receiver:StreamReceiver):Unit
  
}
case class Data(cosas:String)

trait StreamReceiver {
  def receiveData(s:SparqlResults):Unit
}



class QueryEvaluator(config:Properties,actorSystem:ActorSystem=null) {
  private val logger= LoggerFactory.getLogger(this.getClass)  
  val system=if (actorSystem!=null) actorSystem
    else null//ActorSystem("MorphStreams", ConfigFactory.load.getConfig("morphstreams")) 
  val props = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"));
  private val adapterconfig=if (config==null) props else config 
  val adapterid = props.getProperty("siq.adapter");
  val adapterClass = props.getProperty("siq.adapter."+adapterid+".evaluator")
  
  val theClass=try Class.forName(adapterClass)
	catch {case e:ClassNotFoundException =>
	  throw new IllegalArgumentException("Unable to initialize adapter class "+adapterClass, e)}
  val adapter=theClass.getDeclaredConstructor(classOf[Properties],classOf[ActorSystem])
              .newInstance(adapterconfig,system).asInstanceOf[StreamEvaluatorAdapter]
		
  private val queryids=new collection.mutable.HashMap[String,SourceQuery]
  
  def executeQuery(sparqlstr:String,mappingUri:URI)={
    val query=SparqlStream.parse(sparqlstr)
    val trans = new QueryRewriting(props,mappingUri.toString)
    val qt= trans.translate(query)
    
    val rs=adapter.executeQuery(qt)
    val dt=new DataTranslator(List(rs),qt)
    if (query.getConstructTemplate==null){ 
      val sparql=dt.transform     
      //val json=new Gson().toJson(sparql)
      //println(json)
      
      printSparqlResult(sparql)
      sparql
    }
    else {
      val model=dt.translateToModel(query.getConstructTemplate)
      model.write(System.out,RDFFormat.TTL)
      model
    }
  }
  
  def registerQuery(sparqlstr:String,mappingUri:URI)={
    val query=SparqlStream.parse(sparqlstr)
    val trans = new QueryRewriting(props,mappingUri.toString)
    val qt= trans.translate(query)
    val id=adapter.registerQuery(qt)
    queryids.put(id,qt)
    id
  }
  def listenToQuery(sparqlstr:String,mappingUri:URI,receiver:StreamReceiver)={
    val query=SparqlStream.parse(sparqlstr)
    val trans = new QueryRewriting(props,mappingUri.toString)
    val qt= trans.translate(query)
    adapter.listenQuery(qt,receiver)
  }
  
  def pull(id:String)={
    val qt=queryids(id)
    val rs=adapter.pull(id,qt)
    val dt=new DataTranslator(List(rs),qt)
    val sparql=dt.transform     
    printSparqlResult(sparql)
    sparql    
  }
     
 
  def printSparqlResult(sparql:SparqlResults )	{		   
    /*try {
 	  val jax = JAXBContext.newInstance(classOf[Sparql])
 	  val m = jax.createMarshaller
 	  val sr = new StringWriter
 	  m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true)
 	  m.marshal(sparql,sr)
 	  logger.info(sr.toString)
 	} catch {case e:JAXBException=>e.printStackTrace}*/
    logger.info(EvaluatorUtils.serialize(sparql))
  }

}

object EvaluatorUtils{
  def serialize(sparql:SparqlResults)={
    //val sr = new StringWriter
    ResultSetFormatter.asXMLString(sparql.getResultSet)          
  }
  def serializeJson(sparql:SparqlResults)=
    ResultSetFormatter.outputAsJSON(sparql.getResultSet)
  def serializecsv(sparql:SparqlResults)=
    ResultSetFormatter.outputAsCSV(sparql.getResultSet)

/*
   def sparqlString(sparql:Sparql)={
   try {
 	  val jax = JAXBContext.newInstance(classOf[Sparql])
 	  val m = jax.createMarshaller
 	  val sr = new StringWriter
 	  m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true)
 	  m.marshal(sparql,sr)
 	  sr.toString
 	} catch {case e:JAXBException=>e.printStackTrace
 	  null}         
  }*/
}  
