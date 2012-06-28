package es.upm.fi.oeg.morph.stream.gsn
import com.sun.jersey.api.client.Client
import com.sun.jersey.core.util.MultivaluedMapImpl
import com.sun.jersey.api.client.UniformInterfaceException
import java.io.InputStream
import scala.io.Source
import es.upm.fi.dia.oeg.common.ParameterUtils._
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting
import java.net.URI
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import collection.JavaConversions._
import es.upm.fi.dia.oeg.common.Utils
import com.google.gson.Gson


class GsnAdapter {
  val gsnUrl="http://localhost:22001"
  val timeAttribute="timed"  
  def multidata(query:GsnQuery)={  
    
    val c = Client.create
    val webResource = c.resource(gsnUrl+"/multidata?"+query.serializeQuery)
    val queryParams = new MultivaluedMapImpl
    //query.serializeQuery. streams.for{s=>queryParams.add("vs[0]", s.name)}
	//queryParams.add("vs[0]","bizi")
    //queryParams.add("field[0]","free,id")
    //queryParams.add("download_mode","inline")
    
    queryParams.add("nb","SPECIFIED")
    queryParams.add("nb_value","20")

    val wr = webResource.queryParams(queryParams)
	println(wr.getURI().toString())
	val res=try{
	  //wr.get(classOf[InputStream])
	  wr.get(classOf[String])
	} catch {case e:UniformInterfaceException=> throw e}
	println("got data")
	//val s=Source.fromInputStream(res)
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
  val props = load(getClass.getClassLoader.getResourceAsStream("config/config_memoryStore.gsn.properties"));
  val trans = new QueryRewriting(props);    
  val mappingUri=new URI("mappings/citybikes.ttl")
  
  def executeQuery(sparqlstr:String){
     val qt= trans.translate(sparqlstr,mappingUri)
     val rs=multidata(qt.asInstanceOf[GsnQuery])
     
     val dt=new DataTranslator(List(rs),qt)
     val sparql=dt.transform
     val json=new Gson().toJson(sparql)
     println(json)
     Utils.printSparqlResult(sparql)
     /*
     while (rs.next){
       println(rs.getObject(1))
     }*/
     
  }
  
}



class StreamExtent(val name:String,fields:Array[Field])
class Field(val name:String)


