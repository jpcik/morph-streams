package es.upm.fi.oeg.morph.stream.gsn
import java.sql.ResultSet
import com.google.common.collect.Maps
import java.sql.SQLException
import es.upm.fi.dia.oeg.integration.QueryException
import collection.JavaConversions._
import org.w3.sparql.results.Binding
import org.w3.sparql.results.Literal
import java.sql.ResultSetMetaData
import com.hp.hpl.jena.datatypes.RDFDatatype
import es.upm.fi.oeg.siq.tools.XsdTypes
import org.w3.sparql.results.Sparql
import es.upm.fi.oeg.rdf.sparql.SparqlResults
import org.w3.sparql.results.Result
import com.weiglewilczek.slf4s.Logging
import es.upm.fi.oeg.morph.stream.query.SourceQuery
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.ReplaceXpr
import es.upm.fi.oeg.morph.stream.algebra.xpr.VarXpr
import es.upm.fi.oeg.morph.r2rml.R2rmlUtils._


class DataTranslator(results:Seq[ResultSet],query:SourceQuery) extends Logging{
	//private static Logger logger = Logger.getLogger(DataTranslator.class.getName());

  private val postproc=query.asInstanceOf[GsnQuery].expressions
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
			 
  	  //post processing
	/*
	if (!query.supportsPostProc && postproc.filterNot(_._2.isInstanceOf[VarXpr]).contains(columnName)){
	  value = postprocess(rs, postproc(columnName))
	  b.setUri(value.toString)
	}*/
	//else
	  if (value.toString().startsWith("http://"))
	  b.setUri(value.toString.replace(" ","%20"))
	else try 
	  b.setLiteral(createLiteral(value, rs.findColumn(columnName), metaData))
	  catch {
	    case e:SQLException=>throw new QueryException("Cannot find column for "+columnName,e)
	  }
								
	return b
  }
	
  def transform:Sparql =	{
	val temps = Maps.newHashMap[String,String]
	val extents = Maps.newHashMap[String,Object]

	val sparqlResult = SparqlResults.newSparql(query.getProjection.keySet)
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

	

/*
	public Model translateToModel() throws QueryException
	{
		//Map<String,String> modifiers = query.getModifiers();
		Map<String, es.upm.fi.dia.oeg.integration.Template> templates = query.getTemplates();
		//logger.debug(modifiers);
		Map<String,String> temps = Maps.newHashMap();
		Map<String,Object> extents = Maps.newHashMap();

		Model m = ModelFactory.createDefaultModel();
		//TemplateGroup gt = (TemplateGroup)construct;
		Random r = new Random();
		if (results != null)
		{	
			for (ResultSet o : results)
			{				
				Map<Integer,String> extentIds = getExtentIds(o);

				try
				{
					while (o.next()) 
					{
						temps.clear();
						extents.clear();
						if (!templates.isEmpty()) 
						for (Integer i:extentIds.keySet())
						{
							extents.put(extentIds.get(i), o.getObject(i.intValue()));
							temps.putAll(templates.get(o.getObject(i.intValue())).getModifiers());
						}

						HashMap<String,AnonId> bnodes = new HashMap<String,AnonId>();
						long l = r.nextLong();
						//for (Template tmp: gt.getTemplates())
						for (Triple tt:construct.getTriples())
						{							
							//TemplateTriple tt = (TemplateTriple)tmp;
							Resource subject = null;
							RDFNode object = null;
							if (tt.getSubject().isVariable())
							{
								if (query.getConstants().containsKey(tt.getSubject().getName().toLowerCase()))
								{
									subject = m.createResource(query.getConstants().get(tt.getSubject().getName().toLowerCase()));
								}
								else
								{
								Object val;
								try
								{
									val = o.getObject(tt.getSubject().getName().toLowerCase());
								} catch (SQLException e)
								{
									throw new QueryException("Cannot get value for "+tt.getSubject().getName(),e);
								}
								if (temps.containsKey(tt.getSubject().getName().toLowerCase()))
								{
									val = postprocess(val, temps.get(tt.getSubject().getName().toLowerCase()));
									subject = m.createResource(val.toString()+l);
								}
								else
									subject = m.createResource(val.toString());

								}
							}
							else if (tt.getSubject().isBlank())
							{
								if (bnodes.containsKey(tt.getSubject().getBlankNodeLabel()))
								{
									subject = m.createResource(bnodes.get(tt.getSubject().getBlankNodeLabel()));
								}
								else
								{
									subject = m.createResource();
									bnodes.put(tt.getSubject().getBlankNodeLabel(), subject.asNode().getBlankNodeId());
								}
							}
							
							else
							{
								subject = (Resource) tt.getSubject();
							}
							
							
							if (tt.getObject().isVariable())
							{
								if (query.getConstants().containsKey(tt.getObject().getName().toLowerCase()))
								{
									object = m.createResource(query.getConstants().get(tt.getObject().getName().toLowerCase()));
								}
								else
								{
									Object val;
								/*
									if (colIds.get(tt.getTriple().getObject().getName().toLowerCase())==null)
									{
									throw new QueryException("Cannot get value for "+tt.getTriple().getObject().getName().toLowerCase());
									}*/
									val = o.getObject(tt.getObject().getName().toLowerCase());
									if (temps.containsKey(tt.getObject().getName().toLowerCase()))
									{
										val = postprocess(val, temps.get(tt.getObject().getName().toLowerCase()));
										object = m.createResource(val.toString()+l);
									}
									else
									{
									if (val.toString().startsWith("http://"))
										object = m.createResource(val.toString());
									else
										
										object = m.createLiteral(val.toString());
									}
								}
							}
							else
							{
								if (tt.getObject().isLiteral())
									object = m.createLiteral(tt.getObject().getLiteral().getValue().toString());
								else if (tt.getObject().isBlank())
								{
									if (bnodes.containsKey(tt.getObject().getBlankNodeLabel()))
									{
										object = m.createResource(bnodes.get(tt.getObject().getBlankNodeLabel()));
									}
									else
									{
										object = m.createResource();
										bnodes.put(tt.getObject().getBlankNodeLabel(), object.asNode().getBlankNodeId());
									}
								}
								
								else
									object = m.createResource(tt.getObject().getURI());
							}
							Property p = m.createProperty(tt.getPredicate().getURI());
							m.add(subject,p,object);
							
						}
					}
				} catch (DatatypeFormatException e)
				{
					throw new QueryException("Cannot iterate over result set.",e);

				} catch (SQLException e)
				{
					throw new QueryException("Cannot iterate over result set.",e);
				}
					
			}
		}
		return m;
	}

*/

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