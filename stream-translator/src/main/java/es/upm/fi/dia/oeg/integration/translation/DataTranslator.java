package es.upm.fi.dia.oeg.integration.translation;

import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import org.apache.log4j.Logger;
import org.w3.sparql.results.Binding;
import org.w3.sparql.results.Literal;
import org.w3.sparql.results.Result;
import org.w3.sparql.results.Results;
import org.w3.sparql.results.Sparql;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.datatypes.DatatypeFormatException;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.impl.XSDGenericType;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.hp.hpl.jena.sparql.syntax.TemplateGroup;
import com.hp.hpl.jena.sparql.syntax.TemplateTriple;

import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.oeg.rdf.sparql.SparqlResults;

public class DataTranslator 
{
	private static Logger logger = Logger.getLogger(DataTranslator.class.getName());

	private List<ResultSet> results;
	private SourceQuery query;
	private Map<String,Attribute> projectList;
	private Template construct;
	private ResultSetMetaData metaData;
	
	public DataTranslator()
	{		
	}
	
	public DataTranslator(List<ResultSet> results, SourceQuery query)
	{
		this.results = results;
		this.query = query;
	}
	
	public void reset(List<ResultSet> results, SourceQuery query, Map<String,Attribute> projectList)
	{
		setProjectList(projectList);
		setQuery(query);
		setResults(results);
	}
	public void reset(List<ResultSet> results, SourceQuery query, Template construct)
	{
		this.construct = construct;
		setQuery(query);
		setResults(results);
	}
	
	private Map<Integer,String> getExtentIds(ResultSet rs) throws QueryException
	{
		//ResultSetMetaData metaData;
		Map<Integer,String> extentIds = Maps.newHashMap();
		try
		{
			metaData = rs.getMetaData();				
			for (int i=1;i<=metaData.getColumnCount();i++)
			{
				if (metaData.getColumnLabel(i).startsWith("extentname"))
					extentIds.put(i,metaData.getColumnLabel(i));
					
			}
		} catch (SQLException e1) {
			throw new QueryException("Cannot get metadata",e1);
		}
		return extentIds;
	}
	
	public Binding createBinding(ResultSet rs, String columnName, Map<String,String> temps) throws QueryException
	{
		Object value;						
		Binding b = new Binding();
		b.setName(columnName);
		try {
			value = rs.getObject(columnName);
		} catch (SQLException e){
			throw new QueryException("Cannot get value for "+columnName,e);
		} catch (NumberFormatException e){
			throw new QueryException("Cannot get value for "+columnName,e);
		} catch (ArrayIndexOutOfBoundsException e){
			throw new QueryException("Result Set inconsistent, column "+columnName,e);
		}

		if (temps.containsKey(columnName))
		{
			value = postprocess(value, temps.get(columnName));
			b.setUri(value.toString());
		}
		else if (value.toString().startsWith("http://"))
		{
			b.setUri(value.toString());
		}	
		else
		{									
			try {
				b.setLiteral(createLiteral(value, rs.findColumn(columnName), metaData ));
			} catch (SQLException e) {
				throw new QueryException("Cannot find column for "+columnName,e);
			}
		}
						
		return b;
	}
	
	public Sparql transform() throws QueryException
	{
		Map<String, es.upm.fi.dia.oeg.integration.Template> templates = query.getTemplates();
		logger.debug("templates list: "+ templates);
		Map<String,String> temps = Maps.newHashMap();
		Map<String,Object> extents = Maps.newHashMap();

		Sparql sparqlResult = SparqlResults.newSparql(projectList.keySet());
		Results res = sparqlResult.getResults();

		if (results == null)
			return sparqlResult;
		
		for (java.sql.ResultSet rs : results)
		{	
 			Map<Integer,String> extentIds = getExtentIds(rs);
			try
			{
				while (rs.next()) 
				{
					if (!templates.isEmpty())
					for (Integer i:extentIds.keySet())
					{
						extents.put(extentIds.get(i), rs.getObject(i.intValue()).toString().toLowerCase());
						temps.putAll(templates.get(rs.getObject(i.intValue()).toString().toLowerCase()).getModifiers());
					}	

					Result result = new Result();
					for (String columnName:projectList.keySet()) 
					{						
						Binding b = createBinding(rs, columnName, temps);
						result.getBinding().add(b );
					}		
											
					res.getResult().add(result);
					temps.clear();
					extents.clear();
				}
				
			} catch (SQLException e){
				throw new QueryException("Cannot get value for resultset",e);
			}
		
					
		}	
		
	
		
	return sparqlResult;
		
	}

	


	public Model translateToModel() throws QueryException
	{
		//Map<String,String> modifiers = query.getModifiers();
		Map<String, es.upm.fi.dia.oeg.integration.Template> templates = query.getTemplates();
		//logger.debug(modifiers);
		Map<String,String> temps = Maps.newHashMap();
		Map<String,Object> extents = Maps.newHashMap();

		Model m = ModelFactory.createDefaultModel();
		TemplateGroup gt = (TemplateGroup)construct;
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
						for (Template tmp:  gt.getTemplates())
						{							
							TemplateTriple tt = (TemplateTriple)tmp;
							Resource subject = null;
							RDFNode object = null;
							if (tt.getTriple().getSubject().isVariable())
							{
								if (query.getConstants().containsKey(tt.getTriple().getSubject().getName().toLowerCase()))
								{
									subject = m.createResource(query.getConstants().get(tt.getTriple().getSubject().getName().toLowerCase()));
								}
								else
								{
								Object val;
								try
								{
									val = o.getObject(tt.getTriple().getSubject().getName().toLowerCase());
								} catch (SQLException e)
								{
									throw new QueryException("Cannot get value for "+tt.getTriple().getSubject().getName(),e);
								}
								if (temps.containsKey(tt.getTriple().getSubject().getName().toLowerCase()))
								{
									val = postprocess(val, temps.get(tt.getTriple().getSubject().getName().toLowerCase()));
									subject = m.createResource(val.toString()+l);
								}
								else
									subject = m.createResource(val.toString());

								}
							}
							else if (tt.getTriple().getSubject().isBlank())
							{
								if (bnodes.containsKey(tt.getTriple().getSubject().getBlankNodeLabel()))
								{
									subject = m.createResource(bnodes.get(tt.getTriple().getSubject().getBlankNodeLabel()));
								}
								else
								{
									subject = m.createResource();
									bnodes.put(tt.getTriple().getSubject().getBlankNodeLabel(), subject.asNode().getBlankNodeId());
								}
							}
							
							else
							{
								subject = (Resource) tt.getTriple().getSubject();
							}
							
							
							if (tt.getTriple().getObject().isVariable())
							{
								if (query.getConstants().containsKey(tt.getTriple().getObject().getName().toLowerCase()))
								{
									object = m.createResource(query.getConstants().get(tt.getTriple().getObject().getName().toLowerCase()));
								}
								else
								{
									Object val;
								/*
									if (colIds.get(tt.getTriple().getObject().getName().toLowerCase())==null)
									{
									throw new QueryException("Cannot get value for "+tt.getTriple().getObject().getName().toLowerCase());
									}*/
									val = o.getObject(tt.getTriple().getObject().getName().toLowerCase());
									if (temps.containsKey(tt.getTriple().getObject().getName().toLowerCase()))
									{
										val = postprocess(val, temps.get(tt.getTriple().getObject().getName().toLowerCase()));
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
								if (tt.getTriple().getObject().isLiteral())
									object = m.createLiteral(tt.getTriple().getObject().getLiteral().getValue().toString());
								else if (tt.getTriple().getObject().isBlank())
								{
									if (bnodes.containsKey(tt.getTriple().getObject().getBlankNodeLabel()))
									{
										object = m.createResource(bnodes.get(tt.getTriple().getObject().getBlankNodeLabel()));
									}
									else
									{
										object = m.createResource();
										bnodes.put(tt.getTriple().getObject().getBlankNodeLabel(), object.asNode().getBlankNodeId());
									}
								}
								
								else
									object = m.createResource(tt.getTriple().getObject().getURI());
							}
							Property p = m.createProperty(tt.getTriple().getPredicate().getURI());
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



	private static Literal createLiteral(Object value, int columnPos, ResultSetMetaData metaData) throws QueryException
	{
		if (logger.isTraceEnabled())
			logger.trace("createLiteral. value: "+value);
		Literal lit = new Literal();
		lit.setContent(value.toString());
		try
		{								
			lit.setDatatype(toRDFDatatype(metaData.getColumnType(columnPos)).getURI());
		} catch (SQLException e)
		{
			throw new QueryException("Cannot get value data type for "+columnPos,e);
		}
		return lit;
	}
	
	private static Object postprocess(Object value, String modifier)
	{
		if (!modifier.startsWith("CONCAT"))
		{
			int i =modifier.indexOf('{');
			int f = modifier.indexOf('}');
			return modifier.substring(0,i)+value+modifier.substring(f+1);
		}
		CCJSqlParserManager p = new CCJSqlParserManager();
		String val ="";

		
		try
		{
			Select s = (Select) p.parse(new StringReader("SELECT "+ modifier + " FROM temp;"));
			PlainSelect plainSelect = (PlainSelect)s.getSelectBody();
			SelectExpressionItem item = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
			Function f = (Function) item.getExpression();
			if (f.getName().equals("CONCAT"))
			{
				for (Object e :f.getParameters().getExpressions())
				{
					if (e instanceof Column)
					{
						val = val+value;
					}
					else
					{
						String str = e.toString();
						val = val+str.substring(1,str.length()-1);
					}
				}
			}
		} catch (JSQLParserException e)
		{
			logger.error(e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return val;
	}


	private static RDFDatatype toRDFDatatype(int type)
	{
		RDFDatatype dt = com.hp.hpl.jena.datatypes.xsd.impl.XSDGenericType.XSDanyURI;
		switch (type)
		{
		case Types.INTEGER:
			dt = XSDGenericType.XSDinteger;break;
		case Types.VARCHAR:
			dt = XSDGenericType.XSDstring;break;
		case Types.FLOAT:
			dt = XSDGenericType.XSDfloat;break;
		case Types.DATE:
			dt = XSDGenericType.XSDdateTime; break;
		case Types.TIME:
			dt = XSDGenericType.XSDtime; break;
		case Types.TIMESTAMP:
			dt = XSDGenericType.XSDlong; break;
		}
		
		return dt;
	//	return XSDGenericType.XSDinteger;
		
	}

	public void setProjectList(Map<String,Attribute> projectList) {
		this.projectList = projectList;
	}

	public Map<String,Attribute> getProjectList() {
		return projectList;
	}

	public List<ResultSet> getResults() {
		return results;
	}

	public void setResults(List<ResultSet> results) {
		this.results = results;
	}

	public SourceQuery getQuery() {
		return query;
	}

	public void setQuery(SourceQuery query) {
		this.query = query;
	}
}
