package org.rsp.ldp

import play.api.mvc._
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import org.apache.jena.riot.RDFDataMgr
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.{Seq =>JenaSeq}
import collection.JavaConversions._
import scala.util.Random
import java.util.UUID
import org.rsp.jena.JenaPlus
import java.io.StringWriter

 import ModelFactory._
 import JenaPlus._
  
  
object ContainerApp extends Controller {
  
    implicit val m=createDefaultModel
   RDFDataMgr.read(m, "people.ttl")

  def retrieve(id:String) = Action.async {implicit request=>
    val sw=new StringWriter
    val statements=m.listStatements(iri(prefix+id+"/"),null,null)
    val model=createDefaultModel
    statements.foreach(i=>model.add(i))    
    model.write(sw, "TURTLE")
    Future(Ok(sw.toString).as("text/turtle").withHeaders(
        ETAG->tag,
        "Accept-Post"->"text/turtle",
        ALLOW->"GET,POST",
        "Link"-> links.mkString(",") )
    )
  }
 
  def add(id:String)= Action.async{implicit request=>
    
    println(request.body.asText)
    
    Future(Ok("done").as("text/turtle").withHeaders(
        ETAG->tag,
        "Accept-Post"->"text/turtle",
        ALLOW->"GET,POST",
        "Link"-> links.mkString(",") )
    )
  }
 
  private def tag=UUID.randomUUID.toString
  val prefix="http://example.org/"
  val links=Seq("BasicContainer","Resource")
    .map(c=>s"""<http://www.w3.org/ns/ldp#${c}>; rel=\\"type\"""")
  val turtleContent="text/turtle"
}