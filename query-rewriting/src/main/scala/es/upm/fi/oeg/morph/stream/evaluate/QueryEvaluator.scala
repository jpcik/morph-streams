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
import collection.JavaConversions._
import es.upm.fi.oeg.morph.stream.rewriting.QueryReordering
import com.typesafe.config.ConfigException.Missing
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting
import es.upm.fi.oeg.morph.stream.rewriting.OntologyRewriting

/*
trait StreamEvaluatorAdapter {
  
  def executeQuery(abstractQuery:SourceQuery):ResultSet
  def registerQuery(abstractQuery:SourceQuery):String
  def pull(id:String,query:SourceQuery):ResultSet
  def listenQuery(abstractQuery:SourceQuery,receiver:ActorRef):Unit  
}
*/
trait StreamReceiver {
  def receiveData(s:SparqlResults):Unit
}
class QueryEvaluator(systemId:String)  {
  private val logger= LoggerFactory.getLogger(this.getClass)  
  //val system=if (actorSystem!=null) actorSystem
  //  else null//ActorSystem("MorphStreams", ConfigFactory.load.getConfig("morphstreams")) 
  //val defaultprops = ParameterUtils.load(getClass.getClassLoader.getResourceAsStream("config/siq.properties"));
  private val config=ConfigFactory.load.getConfig("morph.streams."+systemId)
  
  
  //val adapterClass = config.getString("adapter."+systemId+".evaluator")
  /*val rewriterClass = try Class.forName(config.getString("rewriter")) catch {
    case e:Missing =>classOf[QueryRewriting]
    case e:Exception =>throw new IllegalArgumentException("Unable to initialize rewriter class ", e)}
  */
  //val theClass=try Class.forName(adapterClass)
  //	catch {case e:ClassNotFoundException =>
  //	  throw new IllegalArgumentException("Unable to initialize adapter class "+adapterClass, e)}
  //val adapter=theClass.getDeclaredConstructor(classOf[String])
    //          .newInstance(systemId).asInstanceOf[StreamEvaluatorAdapter]
  
  private val queryids=new collection.mutable.HashMap[String,SourceQuery]
    
  val caching=try config.getBoolean("rewriter.caching") catch {case e:Missing=>false}
  
  logger.debug("Caching activation: "+caching) 
  
  lazy val cacheQueries=new collection.mutable.HashMap[Int,SourceQuery]  

  protected def i_executeQuery(q:SourceQuery):StreamResultSet=null
  protected def i_registerQuery(q:SourceQuery):String=null
  protected def i_listenToQuery(q:SourceQuery,receiver:StreamReceiver):Unit={}  
  protected def i_pull(id:String,q:SourceQuery):StreamResultSet=null

  
  private def rewriter(mappingUri:URI)={
    //rewriterClass.getDeclaredConstructor(classOf[String],classOf[String])
    //  .newInstance(mappingUri.toString,systemId).asInstanceOf[QueryRewriting]
    
      new QueryRewriting(mappingUri.toString,systemId)
     
      
  }
  
  private def rewrite(sparqlstr:String,mappingUri:URI)={
    def parseAndRewrite(q:String,mapping:URI)={      
      //val reordered=QueryReordering.reorder(SparqlStream.parse(q))
      val reordered=SparqlStream.parse(q)
      //logger.debug("reordered: "+reordered.toString)
      /*if (expanding){
        val expanded=OntologyRewriting.translate(reordered, config.getString("morph.ontology"))
        rewriter(mapping).translate(expanded)
      }
      else*/        
        rewriter(mapping).translate(reordered)
    }
    if (caching){
      logger.info("caching activated")
      val queryhash=sparqlstr.hashCode
      cacheQueries.getOrElseUpdate(queryhash,parseAndRewrite(sparqlstr, mappingUri))
    }
    else 
      parseAndRewrite(sparqlstr, mappingUri)    

  }
  
   def executeQuery(sparqlstr:String,mappingUri:URI)={
    val qt=rewrite(sparqlstr,mappingUri)
    val parsed=SparqlStream.parse(sparqlstr)
    //val rs=adapter.executeQuery(qt)
    val rs=i_executeQuery(qt)
    val dt=new DataTranslator(List(rs),qt)
    if (parsed.getConstructTemplate==null)
      dt.transform               
    else 
      dt.translateToModel(parsed.getConstructTemplate)  
  }
   def registerQuery(sparqlstr:String,mappingUri:URI)={
    val qt=rewrite(sparqlstr,mappingUri)
    //val id=adapter.registerQuery(qt)
    val id=i_registerQuery(qt)
    queryids.put(id,qt)
    id
  }
   def listenToQuery(sparqlstr:String,mappingUri:URI,receiver:StreamReceiver)={
    val qt=rewrite(sparqlstr,mappingUri)
    i_listenToQuery(qt,receiver)
    //adapter.listenQuery(qt,receiver)
  }
  
   def pull(id:String)={
    val qt=queryids(id)
    //val rs=adapter.pull(id,qt)
    val rs=i_pull(id,qt)
    val dt=new DataTranslator(List(rs),qt)
    dt.transform             
  }
     
 
  def printSparqlResult(sparql:SparqlResults )	{		   
    logger.info(EvaluatorUtils.serialize(sparql))
  }

  

  
}

abstract class DataReceiver(rec:StreamReceiver,query:SourceQuery) extends Actor{
  private val logger = LoggerFactory.getLogger(this.getClass)
    
  def resultSet(data:Stream[Array[Object]],query:SourceQuery):StreamResultSet
  def receive={
    case data:Array[Array[Object]]=>
        logger.trace("Array intercepted")
        val rs=resultSet(data.toStream,query)
        val dt=new DataTranslator(List(rs),query)
        rec.receiveData(dt.transform)        
    case m=>logger.debug("got "+m)
        throw new IllegalArgumentException("Stream data receiver got: "+m)
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

}  
