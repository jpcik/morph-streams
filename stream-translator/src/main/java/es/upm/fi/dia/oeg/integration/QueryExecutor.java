package es.upm.fi.dia.oeg.integration;

import java.io.StringReader;
import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.w3.sparql.results.Head;
import org.w3.sparql.results.Literal;
import org.w3.sparql.results.Result;
import org.w3.sparql.results.Results;
import org.w3.sparql.results.Sparql;
import org.w3.sparql.results.Variable;

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

import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.dia.oeg.sparqlstream.StreamQuery;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.adapter.gsn.GsnAdapter;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.adapter.ssg4env.SSG4EnvAdapter;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;




public class QueryExecutor
{
	private SourceAdapter adapter;
	
	//private static QueryExecutor instance;

	//private String metadataPath;
	private static Logger logger = Logger.getLogger(QueryExecutor.class.getName());

	/*
	public String getMetadataPath() {
		return metadataPath;
	}


	public void setMetadataPath(String metadataPath) {
		this.metadataPath = metadataPath;
	}
	*/
	/*
	public static synchronized QueryExecutor getInstance(Properties props) throws IntegratorConfigurationException
	{
		//if (instance==null)
		{
			instance = new QueryExecutor();
			instance.init(props);

		}
		return instance;
	}*/
	
	
	public QueryExecutor(Properties props) throws IntegratorConfigurationException
	{
		init(props);
	}
	
	
	
	public void addPullSource(String url, SourceType type) throws DataSourceException
	{
		try {
			adapter.addPullSource(url, type);
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Bad source url "+url, e);
		}
	}
	
	public void init(String adapterId,Properties props) throws StreamAdapterException
	{
		if (adapterId.equals("snee"))
			adapter = new SNEEAdapter();
		else if (adapterId.equals("ssg4e"))
			adapter = new SSG4EnvAdapter();
		else if (adapterId.equals("gsn"))
			adapter = new GsnAdapter();
		adapter.init(props);		
		
	}
	
	public static final String QUERY_EXECUTOR_ADAPTER = "integrator.queryexecutor.adapter"; 
	
	public void init(Properties props) throws IntegratorConfigurationException
	{
		String adapterId = props.getProperty(QUERY_EXECUTOR_ADAPTER);
		
		try
		{
			init(adapterId,props);
		} catch (StreamAdapterException e)
		{
			throw new IntegratorConfigurationException("Configuration error in source adapter. ", e);
		}
	}
	
	public String createQuery(SourceQuery query) throws QueryException
	{	
			String queryId =adapter.invokeQueryFactory(query.serializeQuery()+";", 0);//TODO change fixed duration, really necessary for factory? :P			
			return queryId;				
	}
	

	public Sparql query(SourceQuery query,Map<String, Attribute> projectList) throws QueryException
	{		
		List<ResultSet> rs = adapter.invokeQuery(query);//TODO change fixed duration
		Sparql sparqlResult = transform(rs,projectList , query.getModifiers(),query.getStaticConstants());
		return sparqlResult;
	}
	
	public Model query(SourceQuery query, Template constructTemplate) throws QueryException
	{		
		List<ResultSet> rs = adapter.invokeQuery(query);
		Model rdf = transform(rs, query, constructTemplate, query.getModifiers());
		return rdf;
	}
	
	
	public Sparql pullData(String queryId, SourceQuery query,Map<String, Attribute> projectList, boolean newest,int max) throws QueryException
	{
		
		List<ResultSet> results = null;
		if (newest)
			results = adapter.pullNewestData(queryId,max);
		else
			results = adapter.pullData(queryId,max);
		Sparql sparqlResults = transform(results, projectList, query.getModifiers(),query.getStaticConstants());
		return sparqlResults;
	}

	public Model pullData(String queryId, SourceQuery query, Template constructTemplate, boolean newest, int max) throws QueryException
	{
		List<ResultSet> results = null;
		if (newest)
			results = adapter.pullNewestData(queryId,max);
		else
			results = adapter.pullData(queryId,max);
		Model rdfResults = transform(results, query, constructTemplate, query.getModifiers());
		return rdfResults;
	}


	private Model transform(List<ResultSet> results, SourceQuery query, Template construct,Map<String, String> modifiers) throws QueryException
	{
		logger.debug(modifiers);

		Model m = ModelFactory.createDefaultModel();
		TemplateGroup gt = (TemplateGroup)construct;
		Random r = new Random();
		if (results != null)
		{	
			for (ResultSet o : results)
			{				
				ResultSetMetaData metaData;
				Map<String,String> colIds = new HashMap<String,String>();
				Map<Integer,String> extentIds = Maps.newHashMap();

				try
				{
					metaData = o.getMetaData();

					for (int i=1;i<=metaData.getColumnCount();i++)
					{
						colIds.put(metaData.getColumnLabel(i),i+"");
						if (metaData.getColumnLabel(i).startsWith("extentname"))
							extentIds.put(i,metaData.getColumnLabel(i));

					}
				} catch (SQLException e1)
				{
					throw new QueryException("Cannot get metadata",e1);
				}

				try
				{
					while (o.next()) 
					{
						Map<String,Object> extents = Maps.newHashMap();
						for (Integer i:extentIds.keySet())
						{
							extents.put(extentIds.get(i), o.getObject(i.intValue()));
						}

						//Object ext = o.getObject(Integer.parseInt(colIds.get("extentname")));

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
									val = o.getObject(Integer.parseInt(colIds.get(tt.getTriple().getSubject().getName().toLowerCase())));
								} catch (SQLException e)
								{
									throw new QueryException("Cannot get value for "+tt.getTriple().getSubject().getName(),e);
								}
								boolean isModifier = false;
								for (Object ext:extents.values())
								{
									if (modifiers.containsKey(tt.getTriple().getSubject().getName().toLowerCase()+ext))
									{
										isModifier=true;
										val = postprocess(val,modifiers.get(tt.getTriple().getSubject().getName().toLowerCase()+ext));
										subject = m.createResource(val.toString()+l);										
									}
								}
								if (!isModifier)
								//else
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
								
									if (colIds.get(tt.getTriple().getObject().getName().toLowerCase())==null)
									{
									throw new QueryException("Cannot get value for "+tt.getTriple().getObject().getName().toLowerCase());
									}
									val = o.getObject(Integer.parseInt(colIds.get(tt.getTriple().getObject().getName().toLowerCase())));
									boolean isModifier = false;
									for (Object ext:extents.values())
									{

										if (modifiers.containsKey(tt.getTriple().getObject().getName().toLowerCase()+ext))
										{
											val = postprocess(val, modifiers.get(tt.getTriple().getObject().getName().toLowerCase()+ext));
											object = m.createResource(val.toString()+l);
											isModifier=  true;
										}
									}
									if (!isModifier)
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

	private Sparql transform(List<ResultSet> results, Map<String,Attribute> projectList, Map<String, String> modifiers, Map<String,String> staticConstants) throws QueryException
	{
		logger.debug("Modifiers list: "+ modifiers);
		logger.debug("Constants list: "+ staticConstants);
		Sparql sparqlResult = new Sparql();
		Head head = new Head();
		Results res = new Results();
		for (Map.Entry<String,Attribute> projEntry:projectList.entrySet())
		{
			Variable var = new Variable();
			var.setName(projEntry.getKey());
			head.getVariable().add(var );
		}
		if (results != null)
		
		for (java.sql.ResultSet o : results)
		{	
			ResultSetMetaData metaData;
			Map<String,String> colIds = new HashMap<String,String>();
			Map<Integer,String> extentIds = Maps.newHashMap();
			try
			{
				metaData = o.getMetaData();
				
				for (int i=1;i<=metaData.getColumnCount();i++)
				{
					colIds.put(metaData.getColumnLabel(i),i+"");
					if (metaData.getColumnLabel(i).startsWith("extentname"))
						extentIds.put(i,metaData.getColumnLabel(i));
						
				}
			} catch (SQLException e1)
			{
				throw new QueryException("Cannot get metadata",e1);
			}
			try
			{
				while (o.next()) 
				{
					Map<String,Object> extents = Maps.newHashMap();
					for (Integer i:extentIds.keySet())
					{
						extents.put(extentIds.get(i), o.getObject(i.intValue()));
					}
					Result result = new Result();
					//logger.debug("columns: "+numCols);
					for (String columnName:projectList.keySet()) 
					{
						
						Object value;
						//String columnName = metaData.getColumnLabel(i);
						
						Binding b = new Binding();
						b.setName(columnName);
							
						//logger.debug(columnName);
						boolean isStaticCons = false;
						for (Object ext:extents.values())
						{
							if (staticConstants.containsKey(columnName+ext))
							{
								isStaticCons = true;
								value = staticConstants.get(columnName+ext);
								b.setUri(value.toString());
							}
							
						}
						if (!isStaticCons)
						{
							try
							{
								value = o.getObject(Integer.parseInt(colIds.get(columnName)));
							} catch (SQLException e)
							{
								throw new QueryException("Cannot get value for "+columnName,e);
							}

							boolean isModifier = false;
							for (Object ext:extents.values())
							{
							if (modifiers.containsKey(columnName+ext))
							{
								isModifier=true;
								value = postprocess(value,modifiers.get(columnName+ext));
								b.setUri(value.toString());
							}
							}
							if (!isModifier)
							{	
								if (value.toString().startsWith("http://"))
								{
									b.setUri(value.toString());
								}	
								else
								{
									Literal lit = new Literal();
									lit.setContent(value.toString());
									try
									{								
										lit.setDatatype(toRDFDatatype(metaData.getColumnType(Integer.parseInt(colIds.get(columnName)))).getURI());
									} catch (SQLException e)
									{
										throw new QueryException("Cannot get value dtata type for "+columnName,e);
									}
									b.setLiteral(lit );
								}
							}
						}
						
						result.getBinding().add(b );
							
						
					}
					res.getResult().add(result);

				}
			} catch (SQLException e)
			{
				throw new QueryException("Cannot get value for resultset",e);
			}
		
					
		}	
		
	
		
	sparqlResult.setHead(head);
	sparqlResult.setResults(res);
	return sparqlResult;
		
	}

	private Object postprocess(Object value, String modifier)
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

	private RDFDatatype toRDFDatatype(int type)
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
		
/*		switch (d)
		{
		case INTEGER:
			dt = XSDGenericType.XSDinteger; break;
		case FLOAT:
			dt = XSDGenericType.XSDfloat; break;
		case DATETIME:
			dt = XSDGenericType.XSDdateTime; break;
		case TIME:
			dt = XSDGenericType.XSDtime; break;
		case STRING:
			dt = XSDGenericType.XSDstring; break;
		}
	*/	
		return dt;
	//	return XSDGenericType.XSDinteger;
		
	}


}
