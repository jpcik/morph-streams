package es.upm.fi.oeg.integration.adapter.esper;

import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;


import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.Statement;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;

public class EsperAdapter implements SourceAdapter
{
    private EPAdministrator epAdministrator;

    private EPRuntime epRuntime;

	public void addPullSource(String url, SourceType type)
			throws MalformedURLException, DataSourceException {
		// TODO Auto-generated method stub
		
	}

	public void init(Properties props) throws StreamAdapterException {

        //Configuration configuration = new Configuration();
        
        EPServiceProvider epService = EPServiceProviderManager.getProvider("benchmark", (Configuration)props.get("configuration"));
        
        epAdministrator = epService.getEPAdministrator();
        //updateListener = new MyUpdateListener();
        //subscriber = new MySubscriber();
        epRuntime = epService.getEPRuntime();
	}

	public String invokeQueryFactory(String query, int duration)
			throws QueryCompilerException, QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> invokeQuery(String query, int duration)
			throws QueryCompilerException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> invokeQuery(SourceQuery query)
			throws QueryCompilerException, QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> pullData(String queryId) throws QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> pullData(String queryId, int max)
			throws QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> pullNewestData(String queryId) throws QueryException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> pullNewestData(String queryId, int max)
			throws QueryException {
		// TODO Auto-generated method stub
		return null;
	}
	/*
	public EsperStatement registerQuery(String query)
	{
		EsperStatement res = new EsperStatement();
		 EPStatement stmt = epAdministrator.createEPL(query, "lalala");
		 res.st = stmt;
		 
         //stmt.addListener(updateListener);
		return res;
	}*/
	
    public void sendEvent(Object event) 
    {
        epRuntime.sendEvent(event);
    }

	public Statement registerQuery(SourceQuery query)
			throws QueryCompilerException, QueryException {
		EsperStatement res = new EsperStatement();
		String q = query.serializeQuery();
		q = q.substring(0,q.indexOf(';'));
		 EPStatement stmt = epAdministrator.createEPL(q, "lalala");
		 res.st = stmt;
		 res.query = (EsperQuery)query;
		 res.spquery = query.getOriginalQuery();
        //stmt.addListener(updateListener);
		return res;
	}
}
