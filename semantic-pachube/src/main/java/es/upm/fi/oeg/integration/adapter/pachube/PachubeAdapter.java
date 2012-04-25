package es.upm.fi.oeg.integration.adapter.pachube;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.MultivaluedMap;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SourceAdapter;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.Statement;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.oeg.integration.adapter.pachube.model.Dataset;
import es.upm.fi.oeg.integration.adapter.pachube.model.Datastream;
import es.upm.fi.oeg.integration.adapter.pachube.model.Environment;

public class PachubeAdapter implements SourceAdapter
{

	private String apikey;
	
	public void addPullSource(String url, SourceType type)
			throws MalformedURLException, DataSourceException {
		// TODO Auto-generated method stub
		
	}

	public void init(Properties props) throws StreamAdapterException {
		// TODO Auto-generated method stub
		Properties pachubeProps=new Properties();
		try {
			pachubeProps.load(getClass().getClassLoader().getResourceAsStream("config/pachube.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		apikey=pachubeProps.getProperty("api.key");
	}

	public String invokeQueryFactory(String query, int duration)
			throws QueryCompilerException, QueryException 
	{
		// TODO Auto-generated method stub
		return null;
	}
	public String invokeQueryFactory(SourceQuery query)
	throws QueryCompilerException, QueryException 
	{
			// 	TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> invokeQuery(String query, int duration)
			throws QueryCompilerException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<ResultSet> invokeQuery(SourceQuery query)
			throws QueryCompilerException, QueryException 
	{
		PachubeQuery q = (PachubeQuery)query;
		PachubeResultSet rs = new PachubeResultSet(q);
		Client c = Client.create();
		Dataset ds  = new Dataset();
		for (Environment e:q.getEnvironments())
		{
			
			WebResource webResource = c.resource("http://api.pachube.com/v2/feeds/"+
					e.getId()+"/datastreams/"+e.getDatastreams().iterator().next().getId());
			MultivaluedMap<String,String> queryParams = new MultivaluedMapImpl();
			queryParams.add("key", apikey);//"c9c8f31503188d651636301d3deda6b295862803f93c2e8034750798b1257f05");
			queryParams.add("start", "2011-09-02T14:01:46Z");
			queryParams.add("end", "2011-09-02T17:01:46Z");
			queryParams.add("interval", "0");
		   //queryParams.add("param2", "val2");
		   
			String res = webResource.queryParams(queryParams).get(String.class);

			Gson g = new Gson();

			Datastream col = g.fromJson(res, Datastream.class);
			System.out.println("current value: "+res);
			Environment en = new Environment();
			en.setId(e.getId());
			Collection<Datastream> dss = Lists.newArrayList();
			dss.add(col);
			en.setDatastreams(dss);
			ds.getResults().add(en);
		}
		rs.addDataset(ds);

		List<ResultSet> list = Lists.newArrayList();
		list.add(rs);
		return list;
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

	@Override
	public Statement registerQuery(SourceQuery query)
			throws QueryCompilerException, QueryException {
		// TODO Auto-generated method stub
		return null;
	}

}
