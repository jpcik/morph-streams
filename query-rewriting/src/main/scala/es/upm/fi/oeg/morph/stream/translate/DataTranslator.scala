package es.upm.fi.oeg.morph.stream.translate

import java.sql.ResultSet
import java.sql.SQLException
import collection.JavaConversions._
import java.sql.ResultSetMetaData
import com.hp.hpl.jena.datatypes.RDFDatatype
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import java.util.Random
import scala.collection.mutable.HashMap
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.datatypes.DatatypeFormatException
import com.hp.hpl.jena.sparql.syntax.Template
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.siq.tools.XsdTypes
import es.upm.fi.oeg.siq.tools.URLTools
import es.upm.fi.oeg.siq.sparql.SparqlResults
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.rdf.model.ResourceFactory
import es.upm.fi.oeg.siq.sparql.SparqlBinding
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

class DataTranslator(results:Seq[ResultSet],query:SourceQuery) {

  private val logger= LoggerFactory.getLogger(this.getClass)

  //private val postproc=query.expressions
  private var  metaData:ResultSetMetaData=_

  private def getExtentIds(rs:ResultSet):Map[Int,String]={
    val extentIds = //Maps.newHashMap[Int,String] 
	try {
	  metaData = rs.getMetaData				
	  (1 to metaData.getColumnCount).filter(metaData.getColumnLabel(_).startsWith("extentname")).map{i=>
		  (i->metaData.getColumnLabel(i))					
	  }.toMap
	} catch {case e1:SQLException=>throw new QueryException("Cannot get metadata",e1)}
	extentIds
  }
	
  private def createBinding(rs:ResultSet, columnName:String):(String,Node)={
	var value=try rs.getObject(columnName)
	  catch {
	    case e:SQLException =>throw new QueryException("Cannot get value for "+columnName,e)
		case e:NumberFormatException=>throw new QueryException("Cannot get value for "+columnName,e)
		case e:ArrayIndexOutOfBoundsException=>throw new QueryException("Result Set inconsistent, column "+columnName+".",e)
	  }
	logger.trace("value as is: "+value)
	if (value!=null){
	  val node=
	    if (value.toString.startsWith("http://"))
	      ResourceFactory.createResource(value.toString)
	    else try 
	      createLiteral(value, rs.findColumn(columnName), rs.getMetaData)
	    catch {
	      case e:SQLException=>throw new QueryException("Cannot find column for "+columnName,e)
	    }	 							
	  (columnName,node.asNode)
	}
	else null
  }
  
  def transform:SparqlResults =	{
    val vars=query.projectionVars.toList
    //val projListKeys=query.projectionVars
    
	if (results == null)
	  return new SparqlResults(vars,Iterator()) 
	val res=new ArrayBuffer[SparqlBinding]	
	results.foreach{rs=>
	  try {
		while (rs.next) {		 
		  val bndng=vars.map{columnName=>createBinding(rs,columnName)}
		    .filter(_!=null)
		  	
 		  val bndgs=new SparqlBinding(bndng.toMap)
		  res+=bndgs
		}
		rs.close				
	  } catch {
	    case e:SQLException =>throw new QueryException("Cannot get value for resultset",e)
	  }							
	}						
	return new SparqlResults(vars,res.toIterator)		
  }


  def translateToModel(template:Template):Model={

    val m = ModelFactory.createDefaultModel
	val r = new Random
	if (results != null){	
      results.foreach{rs=>							
        val extentIds = getExtentIds(rs)
		try {
		  while (rs.next) {
		    val bnodes = new HashMap[String,AnonId]
			val l = r.nextLong();
			template.getTriples.foreach{tt=>							
			  var subject:Resource = null;
			  var obj:RDFNode = null;
			  if (tt.getSubject.isVariable){
							  
			    var valu=try
					URLTools.encode(rs.getObject(tt.getSubject.getName.toLowerCase).toString)
					catch {case e:SQLException =>
								throw new QueryException("Cannot get value for "+tt.getSubject.getName,e)}
				subject = m.createResource(valu.toString)
			  }
			  else if (tt.getSubject.isBlank){
				if (bnodes.containsKey(tt.getSubject.getBlankNodeLabel))
				  subject = m.createResource(bnodes(tt.getSubject.getBlankNodeLabel))
				else {
				  subject = m.createResource
				  bnodes.put(tt.getSubject.getBlankNodeLabel,subject.asNode.getBlankNodeId)
				}
			  }
			  else subject = tt.getSubject().asInstanceOf[Resource];
							
			  if (tt.getObject.isVariable){
				var valo= rs.getObject(tt.getObject.getName.toLowerCase)
							
				if (valo.toString().startsWith("http://"))
				  obj = m.createResource(valo.toString)
				else
				  obj = m.createLiteral(valo.toString)				
			  }
			  else {
				if (tt.getObject.isLiteral)
				  obj = m.createLiteral(tt.getObject.getLiteral.getValue.toString)
				else if (tt.getObject.isBlank){
				  if (bnodes.containsKey(tt.getObject().getBlankNodeLabel()))
					obj = m.createResource(bnodes(tt.getObject.getBlankNodeLabel))
				  else {
					obj = m.createResource
					bnodes.put(tt.getObject().getBlankNodeLabel(), obj.asNode().getBlankNodeId());
				  }
				}								
				else obj = m.createResource(tt.getObject.getURI)
			  }
			
			  val p = m.createProperty(tt.getPredicate.getURI)
			  m.add(subject,p,obj)
							
			}
		  }
		} catch {
		    case e:DatatypeFormatException=>
				throw new QueryException("Cannot iterate over result set.",e);
            case e:SQLException=>
			    throw new QueryException("Cannot iterate over result set.",e);
		}
	  }
     }
	m
  }


  private def createLiteral(value:Object,columnPos:Int,metaData:ResultSetMetaData)={
    val dt=try toRDFDatatype(metaData.getColumnType(columnPos))
	catch {
	  case e:SQLException=>throw new QueryException("Cannot get value data type for "+columnPos,e)}
	ResourceFactory.createTypedLiteral(value.toString,dt)
  }
		
  private def postprocess(rs:ResultSet, xpr:Xpr):Object=xpr match{
    case rep:ReplaceXpr=>
      //val values=rep.vars.map(v=>v.varName->rs.getObject(v.varName))
      //rep.replace(values.toMap)
      rs.getObject(xpr.varNames.head)
    case _=>null
  }

  private def toRDFDatatype(typeid:Int):RDFDatatype ={		
    //println(typeid+"")
	XsdTypes.sqlType2XsdType(typeid)		}

}

class QueryException(msg:String,e:Throwable) extends Exception(msg,e)