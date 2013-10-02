package es.upm.fi.oeg.morph.stream.gsn
import java.net.URI
import java.util.Properties
import scala.Array.canBuildFrom
import scala.io.Source
import es.upm.fi.oeg.morph.common.ParameterUtils.loadQuery
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import es.upm.fi.oeg.morph.stream.algebra.RootOp
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp
import org.apache.commons.lang.NotImplementedException
import akka.actor.ActorSystem
import es.upm.fi.oeg.morph.stream.evaluate.StreamReceiver
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import akka.actor.ActorRef
import es.upm.fi.oeg.morph.stream.evaluate.ComposedResultSet
import dispatch._
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator

class GsnAdapter(systemId:String="gsn") extends QueryEvaluator(systemId) {
  private val logger=LoggerFactory.getLogger(this.getClass)
  private val timeAttribute="timed"  
  //private val c = Client.create
  private val config=ConfigFactory.load.getConfig("morph.streams."+systemId+".adapter")

  val gsnUrl=config.getString("endpoint")
  
  def multidata(query:GsnQuery)={      
    val svc = url(gsnUrl+"/multidata?"+query.serializeQuery)
    svc.addQueryParameter("nb","SPECIFIED")
    svc.addQueryParameter("nb_value","100000")
    logger.debug("Request to Gsn "+svc.url)
    val res = Http(svc OK as.String)     
    if (logger.isTraceEnabled)
      logger.debug(res())
    
    val it= Source.fromString(res()).getLines
    val (vsname,q)=(it.next,it.next)
    val fields=it.next
    logger.debug("fields "+fields)
	
    val fieldNames:Seq[String]=if (fields.trim.size>1)
	  fields.tail.split(',').map(a=>a.toLowerCase)
	  else Seq()
    
    logger.debug("fnames"+fieldNames)
    
	val stream:Stream[Array[Object]] = Stream.continually(it.next).takeWhile(_=>it.hasNext).map(l=>l.split(',').map(_.asInstanceOf[Object]))
	
	logger.debug("filters in  query: "+query.filters.mkString(","))
    val idxs=query.filters.map(f=>(f,f.index(fieldNames)))
	def reduce(stream:Stream[Array[Object]],f:(Filter[Any],Seq[Int]))=stream.filter{t=>      
      f._1.filter(t,f._2.toIterator)
    }
	val filtered:Stream[Array[Object]]=idxs.foldLeft(stream)(reduce)

	new GsnResultSet(filtered,query.queryExpressions)
  }
  
  override def i_executeQuery(query:SourceQuery)={
    val gsnQuery=query.asInstanceOf[GsnQuery]
    gsnQuery.algebra match {
      case root:RootOp=>root.subOp match {
        case proj:ProjectionOp=>multidata(gsnQuery)
        case union:MultiUnionOp=>
          val streams=union.children.values.map{op=>                    
            val q=new GsnQuery(op,gsnQuery.outputMods)
            multidata(q)
          }    
          new ComposedResultSet(streams.toSeq)
      }
      case proj:ProjectionOp=>multidata(gsnQuery)
    }
  }  
  
  override def i_listenToQuery(query:SourceQuery,receiver:StreamReceiver){
    throw new NotImplementedException
  }
  
  override def i_registerQuery(query:SourceQuery)={
    throw new NotImplementedException    
  }
  
  override def i_pull(id:String,query:SourceQuery)={
    throw new NotImplementedException
  }
}


object GsnConsole{
  def main(args:Array[String]){  
    val query=loadQuery(args(0))
    val mappingUri=
      new URI(args(1))
    val gsn = new GsnAdapter(null)
    //gsn.executeQuery(query,mappingUri)
    println("")
  }
}
