package es.upm.fi.dia.oeg.integration.adapter.ssg4env;

import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.RowSetMetaData;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.WebRowSet;
import javax.xml.namespace.QName;

import org.apache.commons.lang.NotImplementedException;
import org.apache.cxf.ws.addressing.AttributedURIType;
import org.apache.log4j.Logger;
import org.easymock.EasyMockSupport;
import org.w3.sparql.results.Binding;
import org.w3.sparql.results.Head;
import org.w3.sparql.results.Literal;
import org.w3.sparql.results.Result;
import org.w3.sparql.results.Results;
import org.w3.sparql.results.Sparql;
import org.w3.sparql.results.Variable;

import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEEException;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.impl.XSDGenericType;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.syntax.Template;
import com.sun.rowset.WebRowSetImpl;


import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import eu.semsorgrid4env.service.stream.StreamPropertyDocumentType;
import eu.semsorgrid4env.service.stream.integration.AddSourceRequestType;
import eu.semsorgrid4env.service.stream.integration.AddSourceResponseType;
import eu.semsorgrid4env.service.stream.integration.IntegrationInterface;
import eu.semsorgrid4env.service.stream.integration.IntegrationMappingService;
import eu.semsorgrid4env.service.stream.integration.SourceDescriptionFault;
import eu.semsorgrid4env.service.stream.integration.SourceDescriptionListType;
import eu.semsorgrid4env.service.stream.integration.SourceDescriptionType;
import eu.semsorgrid4env.service.stream.pull.GetStreamItemRequest;
import eu.semsorgrid4env.service.stream.pull.GetStreamNewestItemRequest;
import eu.semsorgrid4env.service.stream.pull.InvalidCountFault;
import eu.semsorgrid4env.service.stream.pull.InvalidPositionFault;
import eu.semsorgrid4env.service.stream.pull.MaximumTuplesExceededFault;
import eu.semsorgrid4env.service.stream.pull.PullStreamInterface;
import eu.semsorgrid4env.service.stream.pull.PullStreamService;
import eu.semsorgrid4env.service.stream.query.GenericQueryFactoryRequest;
import eu.semsorgrid4env.service.stream.query.QueryInterface;
import eu.semsorgrid4env.service.stream.query.StreamQueryExpressionType;
import eu.semsorgrid4env.service.stream.query.StreamingQueryService;
import eu.semsorgrid4env.service.wsdai.DataResourceAddressListType;
import eu.semsorgrid4env.service.wsdai.DataResourceAddressType;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.DatasetDataType;
import eu.semsorgrid4env.service.wsdai.DatasetType;
import eu.semsorgrid4env.service.wsdai.GenericQueryResponse;
import eu.semsorgrid4env.service.wsdai.GetDataResourcePropertyDocumentRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListRequest;
import eu.semsorgrid4env.service.wsdai.GetResourceListResponse;
import eu.semsorgrid4env.service.wsdai.InvalidConfigurationDocumentFault;
import eu.semsorgrid4env.service.wsdai.InvalidDatasetFormatFault;
import eu.semsorgrid4env.service.wsdai.InvalidExpressionFault;
import eu.semsorgrid4env.service.wsdai.InvalidLanguageFault;
import eu.semsorgrid4env.service.wsdai.InvalidPortTypeQNameFault;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.PropertyDocumentType;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

//import uk.ac.manchester.cs.snee.evaluator.types.Field.DataType;


public class SSG4EnvAdapter extends EasyMockSupport implements SourceAdapter
{
	
		//private boolean mock;
		private static Logger logger = Logger.getLogger(SSG4EnvAdapter.class.getName());
		private String defaultQueryLocation = null;
		private String defaultPullLocation = null;
		private String defaultIntegrationLocation = null;
		private static final String DEFAULT_PULL_LOCATION = "integrator.queryexecutor.adapter.ssg4e.pull";
		private static final String DEFAULT_QUERY_LOCATION = "integrator.queryexecutor.adapter.ssg4e.query";
		private static final String DEFAULT_INTEGRATION_LOCATION = "integrator.queryexecutor.adapter.ssg4e.integration";
		
		private RDFDatatype toRDFDatatype(String datatype)
		{
			RDFDatatype dt = com.hp.hpl.jena.datatypes.xsd.impl.XSDGenericType.XSDanyURI;

			if (datatype.equals("integer"))
				dt = XSDGenericType.XSDinteger;
			else if (datatype.equals("decimal"))
				dt = XSDGenericType.XSDdecimal;
			else if (datatype.equals("varchar"))
				dt = XSDGenericType.XSDstring;
			
			return dt;
			//return XSDGenericType.XSDinteger;
			
		}
	    private static final QName PULL_SERVICE_NAME = 
	    	new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", 
	    			"PullStreamService");
	    private static final QName QUERY_SERVICE_NAME = 
	    	new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", 
	    			"StreamingQueryService");

	    private static final QName INTEGRATION_SERVICE_NAME =
	    	new QName("http://www.semsorgrid4env.eu/namespace/2009/10/IQS/Integration",
	    			"IntegrationMappingService");
	    
	 
	   
		@Override
		public void addPullSource(String url, SourceType type) throws MalformedURLException
		{
			
			URL wsdlURLintegration = null;
			try
			{
				wsdlURLintegration = new URL(defaultIntegrationLocation);
			} catch (MalformedURLException e1)
			{
				e1.printStackTrace();
				//throw new QueryException(e1);
			}
			IntegrationMappingService is = new IntegrationMappingService(wsdlURLintegration,INTEGRATION_SERVICE_NAME);
		
			IntegrationInterface interf = is.getIntegrationInterface();
			AddSourceRequestType request = new AddSourceRequestType();
			SourceDescriptionListType sourceList = new SourceDescriptionListType();
			SourceDescriptionType source = new SourceDescriptionType();
			DataResourceAddressType address = new DataResourceAddressType();
			AttributedURIType addressUri = new AttributedURIType();
			addressUri.setValue(url);
			address.setAddress(addressUri );
			if (type != SourceType.SERVICE)
				throw new NotImplementedException("Only service type permited by SSG4Env service.");
			
			
			//source.setServiceType()
			source.setServiceURL(address );
			//request.setDataResourceAbstractName(value)
			request.setSourceDescriptionList(sourceList );
			try {
				AddSourceResponseType response = interf.addSource(request);
			} catch (NotAuthorizedFault e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataResourceUnavailableFault e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SourceDescriptionFault e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceBusyFault e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidResourceNameFault e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			//if (url.startsWith("udp"))
				//logger.info("Adding udp source: "+url);//TODO udp sources not added to snee
			//else
				
				//snee.addServiceSource("",url,SourceType.PULL_STREAM_SERVICE);
		}

		@Override
		public void init(Properties props)
		{
			this.defaultPullLocation = props.getProperty(DEFAULT_PULL_LOCATION);
			this.defaultQueryLocation = props.getProperty(DEFAULT_QUERY_LOCATION);
			this.defaultIntegrationLocation = props.getProperty(DEFAULT_INTEGRATION_LOCATION);
		}

		public void getPullResourceList(String pullLocation) throws QueryException
		{
		URL wsdlURLpull = null;
		try
		{
			wsdlURLpull = new URL(pullLocation);
		} catch (MalformedURLException e1)
		{
			e1.printStackTrace();
			throw new QueryException(e1);
		}
		PullStreamService pss = new PullStreamService(wsdlURLpull,PULL_SERVICE_NAME);
		PullStreamInterface portPull = pss.getPullStreamInterface();
		GetResourceListRequest request = new GetResourceListRequest();
		try
		{
			GetResourceListResponse list = portPull.getResourceList(request );
			for (DataResourceAddressType add:list.getDataResourceAddress())
			{
				logger.debug("Resource: "+add.getAddress().getValue());
			}		
		} catch (NotAuthorizedFault e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceBusyFault e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
		public void getResourceList(String queryLocation)
		{
			URL queryWsdlURL = null;
			try {
				queryWsdlURL = new URL(queryLocation);//"http://localhost:8080/SNEE-WS-0.0.2/services/QueryInterface?wsdl");
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			StreamingQueryService queryService = new StreamingQueryService(queryWsdlURL,QUERY_SERVICE_NAME);
			QueryInterface queryPort = queryService.getQueryInterface();
			GetResourceListRequest request = new GetResourceListRequest();
			try
			{
				GetResourceListResponse response = queryPort.getResourceList(request );
				for (DataResourceAddressType add:response.getDataResourceAddress())
				{
					logger.debug("Resource: "+add.getAddress().getValue());
				}
			} catch (ServiceBusyFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotAuthorizedFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void getPropertyDocument(String resource,String queryLocation)
		{
			URL queryWsdlURL = null;
			try {
				queryWsdlURL = new URL(queryLocation);//"http://localhost:8080/SNEE-WS-0.0.2/services/QueryInterface?wsdl");
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			long current = System.currentTimeMillis();
			StreamingQueryService queryService = new StreamingQueryService(queryWsdlURL,QUERY_SERVICE_NAME);
			QueryInterface queryPort = queryService.getQueryInterface();
			System.out.println("elapsed "+(System.currentTimeMillis()-current));
			GetDataResourcePropertyDocumentRequest request = new GetDataResourcePropertyDocumentRequest();
			request.setDataResourceAbstractName(resource);
			try
			{
				
				PropertyDocumentType doc = queryPort.getDataResourcePropertyDocument(request);
				logger.debug("Configuration QName: "+ doc.getConfigurationMap().get(0).getConfigurationDocumentQName());
				logger.debug("Message QName: "+doc.getConfigurationMap().get(0).getMessageQName().toString());
				logger.debug("Port QName: "+doc.getConfigurationMap().get(0).getPortTypeQName().toString());
				logger.debug("Abstract Name: "+doc.getDataResourceAbstractName());
				logger.debug("Resource management: "+ doc.getDataResourceManagement());
				logger.debug("Language: "+doc.getLanguageMap().get(0).getLanguageURI());
				logger.debug("Datasets: "+doc.getDataResourceDescription().getContent().get(0));
				
			} catch (ServiceBusyFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotAuthorizedFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataResourceUnavailableFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidResourceNameFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
		public String invokeQueryFactory(String queryString, String queryLocation)
		{
			URL queryWsdlURL = null;
			try {
				queryWsdlURL = new URL(queryLocation);//"http://localhost:8080/SNEE-WS-0.0.2/services/QueryInterface?wsdl");
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			StreamingQueryService queryService = new StreamingQueryService(queryWsdlURL,QUERY_SERVICE_NAME);
			QueryInterface queryPort = queryService.getQueryInterface();
			GenericQueryFactoryRequest queryRequest = new GenericQueryFactoryRequest();
			queryRequest.setDataResourceAbstractName("snee:query");
			StreamQueryExpressionType query = new StreamQueryExpressionType();
			query.setExpression(queryString);//"SELECT DateTime, Timestamp, TAir FROM envdata_sandownpier_met;");
			query.setLanguage("http://snee.cs.manchester.ac.uk/sneeql");
			queryRequest.setExpression(query );

			DataResourceAddressListType resourceAddressList = null;
			try
			{
					resourceAddressList = queryPort.genericQueryFactory(queryRequest);
			} catch (InvalidLanguageFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidConfigurationDocumentFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceBusyFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotAuthorizedFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidExpressionFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidPortTypeQNameFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataResourceUnavailableFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidResourceNameFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println( resourceAddressList.getDataResourceAddress().get(0).getAddress().getValue());
			String address = resourceAddressList.getDataResourceAddress().get(0).getAddress().getValue();
			// TODO Auto-generated method stub
			return address;
			
		}

	

		@Override 
		public String invokeQueryFactory(String query, int duration)
		{
			return invokeQueryFactory(query, defaultQueryLocation);
		}
/*
		public WebRowSet pullData(String queryId, String pullLocation)
		{
			URL wsdlURLpull = null;
			try {
				wsdlURLpull = new URL(pullLocation);
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			PullStreamService pss = new PullStreamService(wsdlURLpull,PULL_SERVICE_NAME);
			PullStreamInterface portPull = pss.getPullStreamInterface();
			//GetStreamItemRequest streamItemRequest = new GetStreamItemRequest();
			
			//streamItemRequest.setDataResourceAbstractName(queryId);
			GenericQueryResponse response = null;
			try
			{
				logger.info("call getStreamNewestItem");
				//response = portPull.getStreamItem(streamItemRequest);
				GetStreamNewestItemRequest streamNewestItemRequest = new GetStreamNewestItemRequest();
				streamNewestItemRequest.setDataResourceAbstractName(queryId);
				//streamNewestItemRequest.setMaximumTuples(new Integer(50));
				
					response = portPull.getStreamNewestItem(streamNewestItemRequest );
			} catch (MaximumTuplesExceededFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidResourceNameFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DataResourceUnavailableFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidCountFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceBusyFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotAuthorizedFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidDatasetFormatFault e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			WebRowSet wrs = null;
			try
			{
				wrs = new WebRowSetImpl();
			} catch (SQLException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try
			{
				
				logger.info("+++"+response.getDataset().getDatasetData().getContent().get(0)+"+++");
				wrs.readXml(new StringReader(response.getDataset().getDatasetData().getContent().get(0).toString()));
			} catch (SQLException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return wrs;
		}

	*/		

		@Override
		public List<ResultSet> invokeQuery(String query, int duration) throws QueryCompilerException
		{
			// TODO SNEE-WS does not accept one shot queries :(
			throw new NotImplementedException("One shot queries not available.");
		}


	private List<ResultSet> pullData(String queryId, boolean newest,Integer max) throws QueryException
	{
		List<java.sql.ResultSet> resultList = null;
		URL wsdlURLpull = null;
		try
		{
			wsdlURLpull = new URL(defaultPullLocation);
		} catch (MalformedURLException e1)
		{
			e1.printStackTrace();
			throw new QueryException(e1);
		}
		PullStreamService pss = new PullStreamService(wsdlURLpull,PULL_SERVICE_NAME);
		PullStreamInterface portPull = pss.getPullStreamInterface();

		GenericQueryResponse rs = null;
		try
		{
			if (newest)
			{
				GetStreamNewestItemRequest request = new GetStreamNewestItemRequest();
				if (max!=null)
					request.setMaximumTuples(max);
				request.setDataResourceAbstractName(queryId);
				rs = portPull.getStreamNewestItem(request);
			}
			else
			{
				GetStreamItemRequest request = new GetStreamItemRequest();				
				if (max!=null)
					request.setMaximumTuples(max);
				request.setDataResourceAbstractName(queryId);
				rs = portPull.getStreamItem(request);
			}
		} catch (MaximumTuplesExceededFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidResourceNameFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (DataResourceUnavailableFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidCountFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (ServiceBusyFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (NotAuthorizedFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidDatasetFormatFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidPositionFault e)
		{
			throw new QueryException("Error executing "+queryId,e);
		}

		WebRowSet wrs = null;
		try
		{
			wrs = new WebRowSetImpl();
		} catch (SQLException e1)
		{
			throw new QueryException("Error executing "+queryId,e1);
		}
		try
		{

			logger.info("+++"+ rs.getDataset().getDatasetData().getContent().get(0)+ "+++");
			wrs.readXml(new StringReader(rs.getDataset().getDatasetData()
					.getContent().get(0).toString()));
		} catch (SQLException e)
		{
			throw new QueryException("Error executing "+queryId,e);
		}

		resultList = new ArrayList<ResultSet>();
		resultList.add(wrs);
		return resultList;
	}

	
	
	public List<ResultSet> pullData2(String queryId, boolean newest,Integer max,String location) throws QueryException
	{
		List<java.sql.ResultSet> resultList = null;
		URL wsdlURLpull = null;
		try
		{
			wsdlURLpull = new URL(location);
		} catch (MalformedURLException e1)
		{
			e1.printStackTrace();
			throw new QueryException(e1);
		}
		PullStreamService pss = new PullStreamService(wsdlURLpull,PULL_SERVICE_NAME);
		PullStreamInterface portPull = pss.getPullStreamInterface();

		GenericQueryResponse rs = null;
		try
		{
			if (newest)
			{
				GetStreamNewestItemRequest request = new GetStreamNewestItemRequest();
				if (max!=null)
					request.setMaximumTuples(max);
				request.setDataResourceAbstractName(queryId);
				rs = portPull.getStreamNewestItem(request);
			}
			else
			{
				GetStreamItemRequest request = new GetStreamItemRequest();				
				if (max!=null)
					request.setMaximumTuples(max);
				request.setDataResourceAbstractName(queryId);
				rs = portPull.getStreamItem(request);
			}
		} catch (MaximumTuplesExceededFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidResourceNameFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (DataResourceUnavailableFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidCountFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (ServiceBusyFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (NotAuthorizedFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidDatasetFormatFault e2)
		{
			throw new QueryException("Error executing "+queryId,e2);
		} catch (InvalidPositionFault e)
		{
			throw new QueryException("Error executing "+queryId,e);
		}

		WebRowSet wrs = null;
		try
		{
			wrs = new WebRowSetImpl();
		} catch (SQLException e1)
		{
			throw new QueryException("Error executing "+queryId,e1);
		}
		try
		{

			logger.info("+++"+ rs.getDataset().getDatasetData().getContent().get(0)+ "+++");
			wrs.readXml(new StringReader(rs.getDataset().getDatasetData()
					.getContent().get(0).toString()));
		} catch (SQLException e)
		{
			throw new QueryException("Error executing "+queryId,e);
		}

		resultList = new ArrayList<ResultSet>();
		resultList.add(wrs);
		return resultList;
	}

	
		@Override
		public List<ResultSet> pullData(String queryId)  throws QueryException
		{
			return pullData(queryId, false,null);
		}

		@Override
		public List<ResultSet> pullNewestData(String queryId) throws QueryException
		{
			return pullData(queryId, true, null);
		}

		@Override
		public List<ResultSet> pullData(String queryId, int max) throws QueryException
		{
			return pullData(queryId, false,max);
		}

		@Override
		public List<ResultSet> pullNewestData(String queryId, int max) throws QueryException
		{
			return pullData(queryId, true, max);
		} 
		
		@Override
		public List<ResultSet> invokeQuery(SourceQuery query)
				throws QueryCompilerException, QueryException
		{
			// TODO Auto-generated method stub
			return null;
		}

		
	
		
}
