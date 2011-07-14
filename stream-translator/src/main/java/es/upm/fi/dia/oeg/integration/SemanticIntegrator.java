package es.upm.fi.dia.oeg.integration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.w3.sparql.results.Sparql;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.rdf.model.Model;

import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.metadata.DataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.integration.registry.FileIntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.integration.registry.MemoryIntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.SQLIntegratorRegistry;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslationException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.sparqlstream.StreamQuery;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;


public class SemanticIntegrator 
{
	public static final String EXECUTE_STREAM_ENGINE = "r2o.model.execute";
	static final String INTEGRATOR_REPOSITORY_PROVIDER = "integrator.repository.provider";
	static final String INTEGRATOR_LOAD_EXTERNAL_RESOURCES = "integrator.load.external.resources";
	public static final String INTEGRATOR_METADATA_MAPPINGS_ENABLED = "integrator.metadata.mappings.enabled";
	
	private static Logger logger = Logger.getLogger(SemanticIntegrator.class.getName());
	//private static Logger demoLog = Logger.getLogger("demo.es.coso");

	private IntegratorRegistry reg;
	private QueryExecutor exe;
	private Set<String> registeredSources = Sets.newHashSet();
	
	public SemanticIntegrator(Properties props) throws IntegratorRegistryException, IntegratorConfigurationException
	{	
		//reg = new MemoryIntegratorRegistry(props);
		String provider = props.getProperty(INTEGRATOR_REPOSITORY_PROVIDER);
		if (provider.equals("file"))
			reg= new FileIntegratorRegistry(props);
		else if (provider.equals("memory"))
			reg = new MemoryIntegratorRegistry(props);
		else if (provider.equals("sql"))
			reg = new SQLIntegratorRegistry(props);
		exe = new QueryExecutor(props);
		String loadExternalResources = props.getProperty(INTEGRATOR_LOAD_EXTERNAL_RESOURCES);
		if (loadExternalResources!=null &&  loadExternalResources.equals("true"))
			loadExternalResources();
				
	}
	
	private void loadExternalResources() throws IntegratorRegistryException, IntegratorConfigurationException
	{
		Set<String> delete = new HashSet<String>();
		for (IntegratedDataSourceMetadata ids:reg.getIntegratedDataResourceCollection())
		{
			for (DataSourceMetadata ds:ids.getSourceList())
			{
				if (ds.getUri() != null && !registeredSources.contains(ds.getUri().toString()))
					try
					{
						addSource(ds);
					} catch (DataSourceException e)
					{
						logger.info("Could not load resource: "+ds.getUri());
						delete.add(ids.getSourceName());
					}
			}
		}
		for (String idsName:delete)
		{
			try
			{
				removeIntegratedSource(idsName);
			} catch (DataSourceException e)
			{logger.error("Could not unload integrated resource: "+idsName);}
		}
		loadPullResources();
	}
	
	private void loadPullResources() throws IntegratorRegistryException,  IntegratorConfigurationException
	{
		for (PullDataSourceMetadata pullDs:reg.getPullDataResourceCollection())
		{
			try
			{
				if (retrieveIntegratedDataSource(pullDs.getVirtualSorceName())!=null)
				{
					removePullDataSource(pullDs.getSourceName());
					QueryDocument queryDoc = new QueryDocument();
					queryDoc.setQueryString(pullDs.getQuery());
					try
					{
						pullQueryFactory(pullDs.getVirtualSorceName(), queryDoc);
					} catch (DataSourceException e)
					{
						logger.error("Unable to load pull query "+pullDs.getQuery());
					} catch (QueryException e)
					{
						logger.error("Could not launch pull resource "+pullDs.getSourceName()+" with query: "+pullDs.getQuery());
					}
				}
			} catch (DataSourceException e)
			{
				logger.error("Unable to load pull queries "+e.getMessage());
			}
		}

	}
	
	public IntegratedDataSourceMetadata integrateAs(List<DataSourceMetadata> sources, 
			 String integratedResourceName, MappingDocumentMetadata mapping) throws  DataSourceException
	{
		IntegratedDataSourceMetadata integratedMd = null;
		String uriRoot = reg.getRegistryProps().getProperty(IntegratorRegistry.INTEGRATOR_VIRTUAL_URIBASE); 
	
		IntegratedDataSourceMetadata imd = retrieveIntegratedDataSource(integratedResourceName);
		if (imd!=null)
		{
			String msg = "Integrated data resource already exists: "+integratedResourceName;
			logger.error(msg);
			throw new ResourceAlreadyExistsException(msg);
		}
		String mappingName = mapping.getName();
		if (mappingName.indexOf('.')>0)
			mappingName = mappingName.substring(0, mappingName.indexOf('.'));
		mapping.setName(mappingName);

		storeMappingDocument(mapping);
		
		
		//String suffix = integratedResourceName.substring(integratedResourceName.lastIndexOf(':')+1);
		try	{
			integratedMd = new IntegratedDataSourceMetadata(integratedResourceName, 
					SourceType.SERVICE, new URI(uriRoot+integratedResourceName));
		} catch (URISyntaxException e1)	{
			throw new InvalidResourceNameException("Invalid integrated resource name, cannot compose Uri:"+ uriRoot+integratedResourceName, e1);
		}
		integratedMd.setMapping(mapping);
		integratedMd.setSourceList(sources);
		try {
			reg.storeIntegratedDataSource(integratedMd);
		} catch (IntegratorRegistryException e)	{
			throw new DataSourceException("Cannot store integrated resource "+integratedResourceName,e);
		}
		for (DataSourceMetadata d:integratedMd.getSourceList())
		{
			if (d.getUri() != null)
			{
				addSource(d);			
			}
		}
		return integratedMd;
	}

	public void storeMappingDocument (MappingDocumentMetadata mapping) throws DataSourceException 
	{
		try
		{
			reg.storeMappingDocument(mapping);
		} catch (IntegratorRegistryException e)
		{
			throw new DataSourceException("Cannot store mapping document "+mapping.getName(),e);
		}
	}

	public Collection<IntegratedDataSourceMetadata> retrieveIntegratedDataSourceCollection() throws DataSourceException 
	{
		try
		{
			return reg.getIntegratedDataResourceCollection();
		} catch (IntegratorRegistryException e)
		{
			throw new DataSourceException("Cannot retrieve integrated resources metadata ",e);
		}
	}

	public IntegratedDataSourceMetadata retrieveIntegratedDataSource(String dataSourceName) throws DataSourceException
	{
		try
		{
			return reg.retrieveIntegratedDataSourceMetadata(dataSourceName);
		} catch (IntegratorRegistryException e)
		{
			throw new DataSourceException("Cannot retrieve integrated resource metadata "+dataSourceName,e);
		}
	}

	public void removeIntegratedSource(String integratedSourceName) throws DataSourceException 
	{
		try
		{
			reg.removeIntegratedDataResource(integratedSourceName);
		} catch (IntegratorRegistryException e)
		{
			throw new DataSourceException("Error removing integrated resource: "+integratedSourceName);
		}
	}

	public void addSource(DataSourceMetadata ds) throws DataSourceException
	{
        exe.addPullSource(ds.getUri().toString(),ds.getType());
        registeredSources.add(ds.getUri().toString());
	}
	public PullDataSourceMetadata pullQueryFactory(String dataResourceName, QueryDocument queryDoc) 
		throws DataSourceException, QueryException		
	{
		QueryTranslator qt = new QueryTranslator(reg.getRegistryProps());	
		logger.info("Received query: "+queryDoc.getQueryString());
		logger.info("Integrated data resource: "+dataResourceName);
		IntegratedDataSourceMetadata intSource = retrieveIntegratedDataSource(dataResourceName);
		if (intSource==null)
		{
			String msg = "Integrated Data resource not found: "+dataResourceName;
			logger.warn(msg);
			throw new InvalidResourceNameException(msg);
		}
		
		SourceQuery query;
		try
		{
		query = qt.translate(queryDoc.getQueryString(), intSource.getMapping().getUri());
		} catch (Exception e)
		{
			throw new QueryTranslationException("Translation error " + e.getMessage()+ " "+e.getClass().getName(),e);
		}
		//logger.info("Query translated: "+ query.serializeQuery());
	    String uriRoot = reg.getRegistryProps().getProperty(IntegratorRegistry.INTEGRATOR_VIRTUAL_URIBASE); 
	    String queryId = exe.createQuery(query);
		
		PullDataSourceMetadata ds = null;
		try
		{
			ds = new PullDataSourceMetadata(intSource.getSourceName()+queryId, 
					SourceType.SERVICE, new URI( uriRoot+intSource.getSourceName()+queryId));
		} catch (URISyntaxException e)
		{
			throw new DataSourceException("Invalid new Resource URI."+e.getMessage(),e);
		}
		ds.setQuery(queryDoc.getQueryString());
		ds.setQueryId(queryId);
		ds.setVirtualSourceUri(intSource.getUri().toString());
		ds.setVirtualSorceName(dataResourceName);
		try
		{
			reg.registerPullDataSource(ds);
		} catch (IntegratorRegistryException e)
		{
			new DataSourceException("Error registering the pull data source for query"+queryId,e);
		} catch (Exception e)
		{
			throw new QueryException("Query exception "+e.getMessage(),e);
		}
		return ds;
		
		
	}

	public ResponseDocument query(String dataResourceName, QueryDocument queryDoc) throws DataSourceException, QueryCompilerException, 
	IntegratorRegistryException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, IntegratorConfigurationException
	//TODO too many exceptions
	{
		ResponseDocument doc = new ResponseDocument();
		
		QueryTranslator qt = new QueryTranslator(reg.getRegistryProps());		
		IntegratedDataSourceMetadata intSource = reg.retrieveIntegratedDataSourceMetadata(dataResourceName);
		if (intSource==null)
		{
			throw new DataSourceException("Integrated Data Resource not found: "+dataResourceName);
		}
		SourceQuery query= qt.translate(queryDoc.getQueryString(), intSource.getMapping().getUri());
		boolean executeSNEE = Boolean.parseBoolean(reg.getRegistryProps().getProperty(EXECUTE_STREAM_ENGINE));
		logger.info("Executing with SNEE: "+executeSNEE);
		Sparql rs = null;
		if (executeSNEE)
		{			
			rs =exe.query(query,qt.getProjectList(queryDoc.getQueryString()));
		}
		
		
		
		doc.setResultSet(rs);
		return doc;
	}
	public ResponseDocument pullData(String dataResourceName) throws DataSourceException, QueryException
	{
		return pullData(dataResourceName, false, Integer.MAX_VALUE);
	}

	public ResponseDocument pullData(String dataResourceName, int max) throws DataSourceException, QueryException
	{
		return pullData(dataResourceName, false, max);
	}
	public ResponseDocument pullNewestData(String dataResourceName, int max) throws DataSourceException, QueryException
	{
		return pullData(dataResourceName, true, max);
	}

	public ResponseDocument pullNewestData(String dataResourceName) throws DataSourceException, QueryException
	{
		return pullData(dataResourceName, true, Integer.MAX_VALUE);
	}

	private ResponseDocument pullData(String dataResourceName,boolean newest, int max) throws DataSourceException, QueryException
	{
		ResponseDocument doc = new ResponseDocument();
		PullDataSourceMetadata pullData = retrievePullDataSource(dataResourceName);
		if (pullData==null)
		{
			String msg = "Pull Data resource not found: "+dataResourceName;
			logger.warn(msg);
			throw new InvalidResourceNameException(msg);
		}
		
		QueryTranslator qt = new QueryTranslator(reg.getRegistryProps());
		logger.info("Received query: "+pullData.getQuery());
		IntegratedDataSourceMetadata is = retrieveIntegratedDataSource(pullData.getVirtualSorceName());
		//Sparql s = exe.executeSNEEqlFactory(pullData.getQueryId(), qt.getProjectList(pullData.getSneeqlQuery()));
		OpInterface op = qt.translateToAlgebra(pullData.getQuery(), is.getMapping().getUri());

		SourceQuery sQuery = qt.transform(op);
		
		//Sparql s = exe.pullData(pullData.getQueryId(), qt.getProjectList(pullData.getSneeqlQuery()));
		StreamQuery query = (StreamQuery) StreamQueryFactory.create(pullData.getQuery());
		if (query.getConstructTemplate() != null)
		{
			Model m =exe.pullData(pullData.getQueryId(),sQuery, query.getConstructTemplate(),newest,max);
			doc.setRdfResultSet(m);
		}
		else
		{
			Sparql s = exe.pullData(pullData.getQueryId(), sQuery,qt.getProjectList(pullData.getQuery()),newest,max);		
			doc.setResultSet(s);
		}
		return doc;
	}
	
	
	public PullDataSourceMetadata retrievePullDataSource(String pullDataSourceString) throws DataSourceException
	{
		try
		{
			return reg.retrievePullDataSourceMetadata(pullDataSourceString);
		} catch (IntegratorRegistryException e)
		{
			throw new DataSourceException("Cannot retrieve pull data source "+pullDataSourceString,e);
		}
	}
	
	public Collection<PullDataSourceMetadata> retrievePullDataSourceCollection() throws IntegratorRegistryException
	{
		return reg.getPullDataResourceCollection();
	}
	
	public void removePullDataSource(String pullDataSourceString) throws DataSourceException, IntegratorRegistryException
	{
		reg.removePullDataSource(pullDataSourceString);
	}
	public void removeAllPullDataSources() throws DataSourceException, IntegratorRegistryException 
	{
		logger.info("Removing all Pull Data resources.");
		reg.removeAllPullDataResources();
	}
	
	
}
