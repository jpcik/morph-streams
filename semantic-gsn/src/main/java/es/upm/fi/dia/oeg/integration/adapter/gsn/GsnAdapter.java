package es.upm.fi.dia.oeg.integration.adapter.gsn;

import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.axis2.AxisFault;
import org.apache.axis2.databinding.types.soapencoding.DateTime;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.ibm.icu.text.DateFormat;

import es.upm.fi.dia.oeg.common.TimeUnit;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.Statement;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import gsn.webservice.standard.GSNWebServiceStub;
import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_DataField;
import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_FieldSelector;
import gsn.webservice.standard.GSNWebServiceStub.GSNWebService_StreamElement;
import gsn.webservice.standard.GSNWebServiceStub.GetLatestMultiData;
import gsn.webservice.standard.GSNWebServiceStub.GetLatestMultiDataResponse;
import gsn.webservice.standard.GSNWebServiceStub.GetMultiData;
import gsn.webservice.standard.GSNWebServiceStub.GetMultiDataResponse;
import gsn.webservice.standard.GSNWebServiceStub.ListVirtualSensorNames;
import gsn.webservice.standard.GSNWebServiceStub.ListVirtualSensorNamesResponse;
import gsn.webservice.standard.GSNWebServiceStub.StandardCriterion;

public class GsnAdapter implements SourceAdapter
{

	private static final String GSN_ENDPOINT = "gsn.endpoint";
	private static Logger logger = Logger.getLogger(GsnAdapter.class.getName());

	GSNWebServiceStub gsn;
	
	@Override
	public void addPullSource(String url,SourceType type) throws MalformedURLException,
			DataSourceException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(Properties props) throws StreamAdapterException
	{
		try
		{
			logger.debug(props.getProperty(GSN_ENDPOINT));
			gsn = new GSNWebServiceStub(props.getProperty(GSN_ENDPOINT));
		} catch (AxisFault e)
		{
			throw new StreamAdapterException("Cannot initialize GSN service adapter.  ", e);
		}
		
	}

	public List<ResultSet> invokeQuery(SourceQuery query) throws QueryException
	{
		GsnQuery gQuery = (GsnQuery)query;
		GetLatestMultiData request = new GetLatestMultiData();
		GetMultiData req = new GetMultiData();

		
		
		logger.debug("counting "+gQuery.getSelectors().length);
		req.setFieldSelector(gQuery.getSelectors());	
		req.setConditions(gQuery.getConditions());
		req.setTimeFormat("unix");

		GetMultiDataResponse response = null;
		try
		{
			if (gQuery.getWindow()!=null)
			{
			Double from = new Double(TimeUnit.convertToUnit(gQuery.getWindow().getFromOffset(), 
					gQuery.getWindow().getFromUnit(), TimeUnit.MILLISECOND));			
			req.setFrom(System.currentTimeMillis()-from.longValue());
			Double to = new Double(TimeUnit.convertToUnit(gQuery.getWindow().getToOffset(), 
					gQuery.getWindow().getToUnit(), TimeUnit.MILLISECOND));			
			req.setTo(System.currentTimeMillis()-to.longValue());
			}
			else
			{
				req.setFrom(1271638922851l);
				req.setTo(System.currentTimeMillis());
			}
			req.setNb(300);
			StandardCriterion crit = new StandardCriterion();
			//crit.se
			//req.addConditions(crit );
			//req.
			logger.debug("From: "+ new Date(req.getFrom())+" to "+new Date(req.getTo()));

			response = gsn.getMultiData(req );//.getLatestMultiData(request);
		} catch (RemoteException e)
		{
			throw new QueryException("Error processing query in gsn. ",e);
		}
		
		logger.debug("results: "+ response.getQueryResult().length);

		for (int i=0;i<response.getQueryResult().length;i++)
		{
			GSNWebService_StreamElement[] streamData = response.getQueryResult()[i].getStreamElements();

			if (streamData==null) continue;
			
			GSNWebService_DataField[] fields = response.getQueryResult()[i].getFormat().getField();
			
			System.out.println(response.getQueryResult()[i].getExecutedQuery());
			System.out.println("Timed "+response.getQueryResult()[i].getFormat().getTimed());
			for(int j=0;j<streamData.length;j++)	
			{
				long longo = Long.parseLong(streamData[j].getTimed());
				Date timed = new Date(longo);
				System.out.println("Timed "+timed);
				for (int k=0;k<streamData[j].getField().length;k++)
				{   
					System.out.println(fields[k].getName());
					System.out.println(streamData[j].getField()[k].getString());
				}
			}
		}

		
		List<ResultSet> rs = new ArrayList<ResultSet>();
		
		ResultSet r = new GsnResultSet(response.getQueryResult(),gQuery);
		rs.add(r);
		return rs;
	}

	@Override
	public String invokeQueryFactory(String query, int duration)
			throws QueryCompilerException, QueryException
	{
				return null;
	}


	@Override
	public List<ResultSet> pullData(String queryId) throws QueryException
	{
		GetLatestMultiData request = new GetLatestMultiData();
		GSNWebService_FieldSelector[] selector = new GSNWebService_FieldSelector[1];
		selector[0] = new GSNWebService_FieldSelector();
		request.setFieldSelector(selector );
		throw new NotImplementedException("Pull data not available in gsn adapter");
		//gsn.getLatestMultiData(request);
	}

	@Override
	public List<ResultSet> invokeQuery(String query, int duration)
			throws QueryCompilerException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ResultSet> pullNewestData(String queryId) throws QueryException
	{
		throw new NotImplementedException("Pull newest data not available in gsn adapter");
	}

	@Override
	public List<ResultSet> pullData(String queryId, int max) throws QueryException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ResultSet> pullNewestData(String queryId, int max) throws QueryException
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
