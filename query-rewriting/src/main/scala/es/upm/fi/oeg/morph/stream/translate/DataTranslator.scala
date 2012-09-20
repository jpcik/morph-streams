package es.upm.fi.oeg.morph.stream.translate

import java.sql.ResultSet
import com.google.common.collect.Maps
import java.sql.SQLException
import collection.JavaConversions._
import java.sql.ResultSetMetaData
import com.hp.hpl.jena.datatypes.RDFDatatype
import com.weiglewilczek.slf4s.Logging
import org.w3.sparql.results.Binding
import org.w3.sparql.results.Sparql
import org.w3.sparql.results.Result
import org.w3.sparql.results.Literal
import org.w3.sparql.results.Variable
import org.w3.sparql.results.Head
import org.w3.sparql.results.Results
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


class DataTranslator(results:Seq[ResultSet],query:SourceQuery) extends Logging{

  //private val postproc=query.expressions
  private var  metaData:ResultSetMetaData=_

  private def getExtentIds(rs:ResultSet):Map[Int,String]={
    val extentIds = Maps.newHashMap[Int,String] 
	try {
	  metaData = rs.getMetaData				
	  (1 to metaData.getColumnCount).foreach{i=>
	    if (metaData.getColumnLabel(i).startsWith("extentname"))
		  extentIds.put(i,metaData.getColumnLabel(i))					
	  }
	} catch {case e1:SQLException=>	throw new QueryException("Cannot get metadata",e1)}
	return extentIds.toMap
  }
	
  private def createBinding(rs:ResultSet, columnName:String,temps:Map[String,String]):Binding={
	val b = new Binding
	b.setName(columnName)
	
	var value=try rs.getObject(columnName)
	  catch {
	    case e:SQLException =>throw new QueryException("Cannot get value for "+columnName,e)
		case e:NumberFormatException=>throw new QueryException("Cannot get value for "+columnName,e)
		case e:ArrayIndexOutOfBoundsException=>throw new QueryException("Result Set inconsistent, column "+columnName,e)
	  }

	logger.debug("value as is: "+value)
  	  //post processing
	/*
	if (!query.supportsPostProc && postproc.filterNot(_._2.isInstanceOf[VarXpr]).contains(columnName)){
	  value = postprocess(rs, postproc(columnName))
	  b.setUri(value.toString)
	}*/
	//else
	  if (value.toString().startsWith("http://"))
	  b.setUri(value.toString)//.replace(" ","%20"))
	else try 
	  b.setLiteral(createLiteral(value, rs.findColumn(columnName), rs.getMetaData))
	  catch {
	    case e:SQLException=>throw new QueryException("Cannot find column for "+columnName,e)
	  }
								
	return b
  }
	
  private def newSparql(varNames:Seq[String])={
	val sparqlResult = new Sparql
	val head = new Head
	val res = new Results
	varNames.foreach{varName=>
	  head.getVariable().add(newVariable(varName) )
	}
	sparqlResult.setHead(head)
	sparqlResult.setResults(res)
	sparqlResult
  }
	
  private def newVariable(varName:String)={
	val vari = new Variable
	vari.setName(varName)
	vari
  }

  
  def transform:Sparql =	{
	val temps = Maps.newHashMap[String,String]
	val extents = Maps.newHashMap[String,Object]

	val sparqlResult = newSparql(query.getProjection.keys.toSeq)
	val res = sparqlResult.getResults

	if (results == null)
	  return sparqlResult
		
	results.foreach{rs=>
 	  val extentIds = getExtentIds(rs)
	  try {
		while (rs.next) {
		  val result = new Result
		  val projectList=query.getProjection
		  projectList.keySet.foreach{columnName=> 	
			val b=createBinding(rs,columnName,temps.toMap)
			result.getBinding.add(b)
		  }													
		  res.getResult.add(result)
		  temps.clear
		  extents.clear
		}
		rs.close				
	  } catch {
	    case e:SQLException =>throw new QueryException("Cannot get value for resultset",e)
	  }
							
	}	
					
	return sparqlResult		
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
							  
							  /*
								if (query.getConstants().containsKey(tt.getSubject().getName().toLowerCase()))
								{
									subject = m.createResource(query.getConstants().get(tt.getSubject().getName().toLowerCase()));
								}
								else
								{*/
							    var valu=
								try
								{
									URLTools.encode(rs.getObject(tt.getSubject.getName.toLowerCase).toString)
								} catch {case e:SQLException =>
									throw new QueryException("Cannot get value for "+tt.getSubject().getName(),e);
								}
								/*if (temps.containsKey(tt.getSubject().getName().toLowerCase()))
								{
									val = postprocess(val, temps.get(tt.getSubject().getName().toLowerCase()));
									subject = m.createResource(val.toString()+l);
								}
								else*/
									subject = m.createResource(valu.toString)

								//}
							}
							else if (tt.getSubject().isBlank())
							{
								if (bnodes.containsKey(tt.getSubject().getBlankNodeLabel()))
								{
									subject = m.createResource(bnodes(tt.getSubject().getBlankNodeLabel()));
								}
								else
								{
									subject = m.createResource();
									bnodes.put(tt.getSubject().getBlankNodeLabel(), subject.asNode().getBlankNodeId());
								}
							}
							
							else
							{
								subject = tt.getSubject().asInstanceOf[Resource];
							}
							
							
							if (tt.getObject().isVariable())
							{/*
								if (query.getConstants().containsKey(tt.getObject().getName().toLowerCase()))
								{
									object = m.createResource(query.getConstants().get(tt.getObject().getName().toLowerCase()));
								}
								else
								{*/
									var valo= rs.getObject(tt.getObject.getName.toLowerCase)
									/*if (temps.containsKey(tt.getObject().getName().toLowerCase()))
									{
										val = postprocess(val, temps.get(tt.getObject().getName().toLowerCase()));
										object = m.createResource(val.toString()+l);
									}
									else
									{*/
									if (valo.toString().startsWith("http://"))
										obj = m.createResource(valo.toString)
									else
										
										obj = m.createLiteral(valo.toString());
									//}
								//}
							}
							else
							{
								if (tt.getObject().isLiteral())
									obj = m.createLiteral(tt.getObject().getLiteral().getValue().toString());
								else if (tt.getObject().isBlank())
								{
									if (bnodes.containsKey(tt.getObject().getBlankNodeLabel()))
									{
										obj = m.createResource(bnodes(tt.getObject().getBlankNodeLabel()));
									}
									else
									{
										obj = m.createResource();
										bnodes.put(tt.getObject().getBlankNodeLabel(), obj.asNode().getBlankNodeId());
									}
								}
								
								else
									obj = m.createResource(tt.getObject().getURI());
							}
							val p = m.createProperty(tt.getPredicate().getURI());
							m.add(subject,p,obj);
							
						}
					}
				} catch {case e:DatatypeFormatException=>
					throw new QueryException("Cannot iterate over result set.",e);

				case e:SQLException=>
					throw new QueryException("Cannot iterate over result set.",e);
				}
					
			}
		}
		return m;
	}



  private def createLiteral(value:Object,columnPos:Int,metaData:ResultSetMetaData)={
	val lit=new Literal
	lit.setContent(value.toString)
	try	lit.setDatatype(toRDFDatatype(metaData.getColumnType(columnPos)).getURI)
	catch {
	  case e:SQLException=>throw new QueryException("Cannot get value data type for "+columnPos,e)}
	lit
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