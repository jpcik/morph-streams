package es.upm.fi.dia.oeg.integration.adapter.snee;

import java.net.MalformedURLException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

import javax.xml.ws.WebServiceException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.Statement;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.ResultStoreImpl;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.evaluator.types.Output;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.types.Duration;

public class SNEEAdapter implements SourceAdapter, Observer
{
	
	private static final String SNEE_PROPERTIES = "snee.properties";


	private static Logger logger = Logger.getLogger(SNEEAdapter.class.getName());

	
	private SNEEController snee;

	
	private Map<String,Integer> ids = new HashMap<String,Integer>();
	
	@Override
	public void init(Properties props) throws StreamAdapterException
	{
	
		String sneeProperties = props.getProperty(SNEE_PROPERTIES);
		init(sneeProperties);
	}
	
	public void init(String sneeProperties) throws StreamAdapterException
	{
		if (logger.isDebugEnabled()){
			logger.debug("ENTER init() with "+sneeProperties);
		}
		if (sneeProperties==null)
			sneeProperties = "snee/snee.properties";
		try
		{
			snee = new SNEEController(sneeProperties);
		} catch (SNEEException e)
		{
			throw new StreamAdapterException("Error Initializing SNEE adapter. ", e);
		} catch (SNEEConfigurationException e)
		{
			throw new StreamAdapterException("Error Initializing SNEE adapter configuration. ", e);
		}
	}
	
	@Override
	public void addPullSource(String url, SourceType type) throws MalformedURLException, DataSourceException	
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("ENTER addSource() with " + url + " ");
		}
		try
		{
			logger.debug("Extents before add: " +snee.getExtentNames());
			if (type==SourceType.UDP)
				logger.info("Adding udp source: "+url);//TODO udp sources not added to snee
			else if (type==SourceType.WSDAIR)
				snee.addServiceSource("", url, uk.ac.manchester.cs.snee.metadata.source.SourceType.WSDAIR);
			else
				snee.addServiceSource("",url,
						uk.ac.manchester.cs.snee.metadata.source.SourceType.PULL_STREAM_SERVICE);
			logger.debug("Extents after add: " +snee.getExtentNames());
			
		} catch (SNEEDataSourceException e)
		{
			throw new DataSourceException("Unable to add source: "+url, e);
		} catch (MetadataException e)
		{
			throw new DataSourceException("Unable to add source: "+url,e);
		} catch (WebServiceException e) {
			throw new DataSourceException("Unable to add source web service: "+url,e);
		}
		
	}



	public List<java.sql.ResultSet> callSNEE(String stringQuery, int time) throws QueryCompilerException
	{
		List<java.sql.ResultSet> results = null;
		
		try 
		{			
			long duration = time;
			logger.info("Query: " + stringQuery);
			
			int queryId = -1;
			try
			{
				queryId = snee.addQuery(stringQuery,null);
			} catch (SNEECompilerException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (MetadataException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (SNEEConfigurationException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//long startTime = System.currentTimeMillis();
			//long endTime = (long) (startTime + (duration * 1000));
			//boolean queryExecuting = true;
			logger.info("Sleeping for " + duration + 
					" seconds, the duration of query execution.");
			
			
			ResultStoreImpl resultSet = 
				(ResultStoreImpl) snee.getResultStore(queryId);
			resultSet.addObserver(this);
			
			/*
			try {			
				Thread.currentThread().sleep((long)duration * 1000);
			} catch (InterruptedException e) {
			}
			
			while (System.currentTimeMillis() < endTime) {
				Thread.currentThread().yield();
			}
			*/
			ResultStore resultSet1 = snee.getResultStore(queryId);
			results = resultSet1.getResults();
			//		Collection<Output> results2 = controller.getResults(queryId2);
			System.out.println("Stopping query " + queryId + ".");
			snee.removeQuery(queryId);
			//		System.out.println("Query run for required duration. " +
			//				"Stopping query " + queryId2 + ".");
			//		controller.removeQuery(queryId2);
							
			 //results = resSet.getResults();
			logger.info("Query run for required duration. " +
					"Stopping query " + queryId + "." +results.size());
			//snee.removeQuery(queryId);
			//snee.close();
			/*
			for (Output output : results) {
				System.out.println(output);
			}*/
			
			
		} catch (SNEEException e) {
			logger.error("SNEE Exception: ",e);
			e.printStackTrace();			
			throw new QueryCompilerException("SNEE execution error.");
		}
		catch (EvaluatorException e) {
			logger.error("SNEE Evaluation Exception: ",e);
			e.printStackTrace();
			throw new QueryCompilerException("SNEE evaluator error.");
		}
		
		return results;
		
	}

	public String getDetails(String extentName) throws ExtentDoesNotExistException
	{
		ExtentMetadata meta = snee.getExtentDetails(extentName);
		logger.debug("Extent "+extentName);
		for (Attribute at:meta.getAttributes())
		{
			logger.debug(at.getAttributeSchemaName()+"--"+at.getAttributeTypeName());
		}
		return null;
	}
	

	public String invokeQueryFactory(String queryString, int duration) throws QueryException
	{
		logger.info("Invoking query: "+queryString);
		logger.info("Extents: "+snee.getExtentNames().size()+"--"+snee.getExtentNames());
		int num=-1;
		try
		{
			num = snee.addQuery(queryString,null); 
		}			// TODO refine exception handling
 
		catch (SNEEException e)
		{
			throw new QueryException(e);
		} catch (EvaluatorException e)
		{
			throw new QueryException(e);
		} catch (SNEECompilerException e)
		{
			throw new QueryCompilerException(e);
		} catch (MetadataException e)
		{
			throw new QueryException(e);
		} catch (SNEEConfigurationException e)
		{
			throw new QueryException(e);
		}
		ids.put(""+num, new Integer(num));
		return ""+num;

	}
	
	
	@Override
	public String invokeQueryFactory(SourceQuery query) throws QueryException 
	{
		return invokeQueryFactory(query.serializeQuery(), 0);
	}

	@Override
	public List<ResultSet> pullNewestData(String queryId) throws QueryException
	{
		return pullData(queryId, true,Integer.MAX_VALUE);
	}

	@Override
	public List<ResultSet> pullNewestData(String queryId, int max) throws QueryException
	{
		return pullData(queryId, true,max);
	}

	@Override
	public List<ResultSet> pullData(String queryId, int max) throws QueryException
	{
		return pullData(queryId,false,max);
	}

	
	@Override
	public List<ResultSet> pullData(String queryId) throws QueryException
	{
		return pullData(queryId,false,Integer.MAX_VALUE);
	}


	private List<ResultSet> pullData(String queryId, boolean newest, int max) throws QueryException
	{
		List<java.sql.ResultSet> resultList = null;
		try
		{		
			ResultStore rs =  snee.getResultStore(ids.get(queryId).intValue());
			if (newest)
			{
				if (rs.size()<max)
					resultList = rs.getNewestResults();
				else
					resultList = rs.getNewestResults(max);
			}
			else
			{
				if (rs.size()<max)
					resultList = rs.getResults();
				else
					resultList = rs.getResults(max);
			}
		}
		catch (IndexOutOfBoundsException e)
		{
			logger.info("No data retrieved from query: "+queryId);
		}
		catch (SNEEException e)
		{
			throw new QueryException(e);
		}
		return resultList;
	}
	
	
	@Override
	public List<ResultSet> invokeQuery(String query, int duration) throws QueryCompilerException
	{
		if (logger.isDebugEnabled())
		{
			logger.debug("ENTER invokeQuery() with " + query +" " +duration);
		}
		//String stringQuery = query.serializeQuery()+";";
		throw new NotImplementedException("One-shot queries not supported by provider.");
	}

	

	@SuppressWarnings("unchecked")
	@Override
	public void update(Observable obs, Object arg)
	{
		//if (logger.isDebugEnabled())
		//	logger.debug("ENTER update() with " + observation + " " + arg);
		//logger.trace("arg type: " + arg.getClass());
		if (arg instanceof List<?>) {
			List<ResultSet> results = (List<ResultSet>) arg;
			try
			{
				printResults(results);
			} catch (SQLException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (arg instanceof Output) {
			Output output = (Output) arg;
			System.out.println(output);
		}
		//if (logger.isDebugEnabled())
		//	logger.debug("RETURN update()");
		
	}
	
	
	public static void printResults(List<ResultSet> results) throws SQLException {
		System.out.println("\n\n************ Results for query  ************\n\n");
		for (ResultSet rs : results) {
			ResultSetMetaData metaData = rs.getMetaData();
			int numCols = metaData.getColumnCount();
			//printColumnHeadings(metaData, numCols);
			while (rs.next()) {
				StringBuffer buffer = new StringBuffer();
				for (int i = 1; i <= numCols; i++) {
					Object value = rs.getObject(i);
					if (metaData.getColumnType(i) == 
						Types.TIMESTAMP && value instanceof Long) {
						buffer.append(
								new Date(((Long) value).longValue()));
					} else {
						buffer.append(value);
					}
					buffer.append("\t");
				}
				System.out.println(buffer.toString());
			}
		}
		System.out.println("\n\n*********************************\n\n");
	}

	@Override
	public List<ResultSet> invokeQuery(SourceQuery query)
			throws QueryCompilerException, QueryException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statement registerQuery(SourceQuery query)
			throws QueryCompilerException, QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	}
