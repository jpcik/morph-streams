package es.upm.fi.oeg.integration.adapter.esper;

import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


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

    private Map<String,EsperQuery> queries;
	private static Logger logger = Logger.getLogger(EsperAdapter.class.getName());

    
	public void addPullSource(String url, SourceType type)
			throws MalformedURLException, DataSourceException {
		// TODO Auto-generated method stub
		
	}

	public void init(Properties props) throws StreamAdapterException 
	{
        EPServiceProvider epService = EPServiceProviderManager.getProvider("benchmark", (Configuration)props.get("configuration"));
        queries = Maps.newHashMap();
        epAdministrator = epService.getEPAdministrator();
        //updateListener = new MyUpdateListener();
        //subscriber = new MySubscriber();
        epRuntime = epService.getEPRuntime();
	}


	@Override
	public String invokeQueryFactory(SourceQuery query) {
		String q  = query.serializeQuery();//.substring(0,query.indexOf(';'));
		 EPStatement stmt = epAdministrator.createEPL(q);
		 //res.st = stmt;
		 //res.query = (EsperQuery)query;
		 
		 //res.spquery = q;
		 EsperStatement res = new EsperStatement(stmt,(EsperQuery)query,query.getOriginalQuery());
		 queries.put(stmt.getName(), (EsperQuery)query);
		 logger.debug("invokeQueryFactory. Created query: "+stmt.getName());
		return stmt.getName();
	}

	public String invokeQueryFactory(String query, int duration)
	{
		query = query.substring(0,query.indexOf(';'));
		 EPStatement stmt = epAdministrator.createEPL(query);
		 //res.st = stmt;
		 //res.query = (EsperQuery)query;
		 
		// res.spquery = query;
		 //queries.put(stmt.getName(), )
	 //EsperStatement res = new EsperStatement(stmt,(EsperQuery)query,query);
			
		return stmt.getName();
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

	public List<ResultSet> pullNewestData(String queryId) throws QueryException 
	{
		logger.debug("pullNewestData. queryId: "+queryId);
		EPStatement st = epAdministrator.getStatement(queryId);
		Iterator<EventBean> it = st.safeIterator();
		/*
		for (;it.hasNext();)
		{
			EventBean event = it.next();
			//System.out.println("event: "+event);
		}*/
		
		EsperResultSet rs = new EsperResultSet(it, queries.get(queryId));
		
		
		List<ResultSet> list = Lists.newArrayList();
		list.add(rs);
		return list;
	}

	public List<ResultSet> pullNewestData(String queryId, int max) throws QueryException
	{
		return pullNewestData(queryId);
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
    
    public void sendEvent(Map<String,Object> event,String et)
    {
    	epRuntime.sendEvent(event, et);
    }

	public Statement registerQuery(SourceQuery query)
			throws QueryCompilerException, QueryException {
		String q = query.serializeQuery();
		 EPStatement stmt = epAdministrator.createEPL(q);
		EsperStatement res = new EsperStatement(stmt,(EsperQuery)query,query.getOriginalQuery());
        //stmt.addListener(updateListener);
		return res;
	}
}
