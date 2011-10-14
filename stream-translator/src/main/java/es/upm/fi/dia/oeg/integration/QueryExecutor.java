package es.upm.fi.dia.oeg.integration;

import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import org.apache.log4j.Logger;
import org.w3.sparql.results.Sparql;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.syntax.Template;

import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.adapter.ssg4env.SSG4EnvAdapter;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.integration.translation.DataTranslator;




public class QueryExecutor
{
	private SourceAdapter adapter;
	private DataTranslator translator;
	
	private static Logger logger = Logger.getLogger(QueryExecutor.class.getName());

	public static final String QUERY_EXECUTOR_ADAPTER = "integrator.queryexecutor.adapter";

	
	public SourceAdapter getAdapter()
	{
		return adapter;
	}
	
	public QueryExecutor(Properties props) throws IntegratorConfigurationException
	{
		init(props);
		translator = new DataTranslator();
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
		else //if (adapterId.equals("gsn"))
		{
			String adapterClass = props.getProperty(QUERY_EXECUTOR_ADAPTER+"."+adapterId+".executor");
			@SuppressWarnings("rawtypes")
			Class theClass;
			try {
				theClass = Class.forName(adapterClass);
			} catch (ClassNotFoundException e) {
				throw new StreamAdapterException("Unable to initialize adapter class "+adapterClass, e);
			}
			try {
				adapter = (SourceAdapter) theClass.newInstance();
			} catch (InstantiationException e) {
				throw new StreamAdapterException("Unable to instantiate adapter class "+adapterClass, e);

			} catch (IllegalAccessException e) {
				throw new StreamAdapterException("Unable to instatiate adapter class "+adapterClass, e);
			}
		}
		adapter.init(props);		
		
	}
	
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
	
	public Statement registerQuery(SourceQuery query) throws QueryCompilerException, QueryException
	{
		return adapter.registerQuery(query);
	}
	
	public String createQuery(SourceQuery query) throws QueryException
	{	
			String queryId =adapter.invokeQueryFactory(query);//TODO change fixed duration, really necessary for factory? :P			
			return queryId;				
	}
	

	public Sparql query(SourceQuery query,Map<String, Attribute> projectList) throws QueryException
	{		
		List<ResultSet> rs = adapter.invokeQuery(query);
		translator.reset(rs, query, projectList);
		Sparql sparqlResult = translator.transform();
		return sparqlResult;
	}
	
	public Model query(SourceQuery query, Template constructTemplate) throws QueryException
	{		
		List<ResultSet> rs = adapter.invokeQuery(query);
		translator.reset(rs, query, constructTemplate);
		Model rdf = translator.translateToModel();
		return rdf;
	}

	
	public Sparql pullData(String queryId, SourceQuery query,Map<String, Attribute> projectList, boolean newest,int max) throws QueryException
	{
		
		List<ResultSet> results = null;
		if (newest)
			results = adapter.pullNewestData(queryId,max);
		else
			results = adapter.pullData(queryId,max);
		translator.reset(results, query, projectList);
		Sparql sparqlResults = translator.transform();
		return sparqlResults;
	}

	public Model pullData(String queryId, SourceQuery query, Template constructTemplate, boolean newest, int max) throws QueryException
	{
		List<ResultSet> results = null;
		if (newest)
			results = adapter.pullNewestData(queryId,max);
		else
			results = adapter.pullData(queryId,max);
		translator.reset(results, query, constructTemplate);
		Model rdfResults = translator.translateToModel();
		return rdfResults;
	}



}
