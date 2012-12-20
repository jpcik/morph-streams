package es.upm.fi.oeg.morph.stream.gsn
import java.net.URI
import java.util.Properties
import scala.Array.canBuildFrom
import scala.io.Source
import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.UniformInterfaceException
import com.sun.jersey.core.util.MultivaluedMapImpl
import com.weiglewilczek.slf4s.Logging
import es.upm.fi.oeg.morph.common.ParameterUtils.loadQuery
import es.upm.fi.oeg.morph.stream.evaluate.StreamEvaluatorAdapter
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import org.apache.commons.lang.NotImplementedException
import akka.actor.ActorSystem


class GsnAdapter(props:Properties,actorSystem:ActorSystem) extends StreamEvaluatorAdapter with Logging{
  private val timeAttribute="timed"  
  private val c = Client.create 
  val gsnUrl=props.getProperty("gsn.endpoint")
  
  def multidata(query:GsnQuery)={      
    val webResource = c.resource(gsnUrl+"/multidata?"+query.serializeQuery)
    val queryParams = new MultivaluedMapImpl
	//queryParams.add("vs[0]","bizi")
    //queryParams.add("field[0]","free,id")
    //queryParams.add("download_mode","inline")    
    queryParams.add("nb","SPECIFIED")
    queryParams.add("nb_value","100")

    val wr = webResource.queryParams(queryParams)
	println(wr.getURI().toString())
	val res=try wr.get(classOf[String])
	catch {case e:UniformInterfaceException=> throw e}

    val s= Source.fromString(res)
    val it=s.getLines()    
    println("vsname "+ it.next()) 
    println("query "+it.next())
    println(query.vars.mkString)
    val fields=it.next
    println("fields "+fields)
	
    if (fields.trim.size>1){
	  val fieldNames=fields.tail.split(',').map(a=>(a.toLowerCase->a))
	  println(fieldNames.mkString("-"))
	}
	val stream = Stream.continually(it.next).takeWhile(_=>it.hasNext).map(_.split(','))
	println("stream created")
	new GsnResultSet(stream,query.expressions)
  }
  
  def executeQuery(query:SourceQuery)={
    val gsnQuery=query.asInstanceOf[GsnQuery]
    gsnQuery.algebra match {
      case root:RootOp=>root.subOp match {
        case proj:ProjectionOp=>multidata(gsnQuery)
        case union:MultiUnionOp=>
          val streams=union.children.values.map{op=>                    
            val q=new GsnQuery(gsnQuery.projectionVars)
            q.load(op)
            multidata(q)
          }
          //val recs=streams.map(_.records).reduce(_++_)
          new GsnMultiResultSet(streams.toArray)
      }
      case proj:ProjectionOp=>multidata(gsnQuery)
    }
  }  
  
    def registerQuery(query:SourceQuery)={
    throw new NotImplementedException    
  }
  
  def pull(id:String,query:SourceQuery)={
    throw new NotImplementedException
  }
}

class StreamExtent(val name:String,fields:Array[Field])
class Field(val name:String)


object GsnConsole{
  def main(args:Array[String]){  
    val query=loadQuery(args(0))
    val mappingUri=
      new URI(args(1))
    val gsn = new GsnAdapter(null,null)
    //gsn.executeQuery(query,mappingUri)
    println("")
  }
}
