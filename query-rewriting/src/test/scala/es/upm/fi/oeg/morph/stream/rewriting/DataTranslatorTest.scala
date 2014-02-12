package es.upm.fi.oeg.morph.stream.rewriting

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.siq.tools.ParameterUtils._
import es.upm.fi.oeg.morph.stream.translate.DataTranslator
import es.upm.fi.oeg.morph.stream.algebra.xpr._
import es.upm.fi.oeg.morph.stream.evaluate.StreamResultSet
import es.upm.fi.oeg.morph.stream.query.SqlQuery
import es.upm.fi.oeg.sparqlstream.SparqlStream
import collection.JavaConversions._

class DataTranslatorTest extends FlatSpec with Matchers  {
  private val logger= LoggerFactory.getLogger(this.getClass)
  val trans = new QueryRewriting("mappings/testMapping.ttl","default")
  val transCCO = new QueryRewriting("mappings/cco.ttl","default")
  def query(q:String)=loadQuery("queries/"+q+".sparql")

  def translate(sparqlstr:String, data:Stream[Array[Object]],qr:QueryRewriting)={
    val parsed=SparqlStream.parse(sparqlstr)
    val q=qr.translate(sparqlstr).asInstanceOf[SqlQuery]    
    val rs=new TestResultSet(data,q.queryExpressions)  
    val dt=new DataTranslator(Seq(rs),q)
    val m=dt.translateToModel(parsed.getConstructTemplate)
    m.write(System.out,"TTL")
    
  }
  "flat construct query result" should "translate to RDF model" in{
    val sparqlstr=query("testConstructSimple")    
    val data:Stream[Array[Object]]=
      Seq(Array("09:00",91,1,11),
          Array("09:01",92,2,22),
          Array("09:02",93,3,33),
          Array("09:03",94,4,44),
          Array("09:04",95,5,55)).map(_.asInstanceOf[Array[Object]]).toStream                 
     translate(sparqlstr,data,trans)
  }

  "nested vars construct query result" should "translate to RDF model" in{
    val sparqlstr=query("constructCCOWaveHeight")    
    val data:Stream[Array[Object]]=
      Seq(Array("09:00",91,1,"11 00",11,111),
          Array("09:01",92,2,"22 00",12,222),
          Array("09:07",97,7,"22 00",17,227),
          Array("09:02",93,3,"33 00",13,333),
          Array("09:03",94,4,"44 00",14,444),
          Array("09:04",95,5,"55 00",15,555)).map(_.asInstanceOf[Array[Object]]).toStream                 
     translate(sparqlstr,data,transCCO)
  }

  
  class TestResultSet(override val records:Stream[Array[Object]], 
    override val metadata: Map[String, Xpr]) extends StreamResultSet {
     override val queryVars=(metadata.map(m=>m._2.varNames).flatten.toSet.filterNot(_.equals("timed"))
    .toList++List("timed")).toSeq
  
    
    override def getObject(columnLabel:String):Object={
    
    metadata(columnLabel) match{
      case rep:ReplaceXpr=>
        rep.evaluate(internalLabels.map(l=>l._1->current(l._2)).toMap)
      case v:VarXpr=>current(internalLabels(v.varName))
      case c:ConstantXpr=>c.evaluate
      case NullValueXpr=>null
    }
  }
  }
}