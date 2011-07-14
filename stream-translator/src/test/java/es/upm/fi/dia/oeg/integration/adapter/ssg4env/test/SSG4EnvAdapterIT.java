package es.upm.fi.dia.oeg.integration.adapter.ssg4env.test;


import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.rowset.WebRowSet;

import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.adapter.ssg4env.SSG4EnvAdapter;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.integration.test.SemanticIntegratorTest;

public class SSG4EnvAdapterIT
{

	private static SSG4EnvAdapter adapter;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		PropertyConfigurator.configure(SemanticIntegratorTest.class.getClassLoader().getResource("config/log4j.properties"));

		adapter = new SSG4EnvAdapter();
	}

	@Test@Ignore
	public void testGetResourceList() throws MalformedURLException
	{
		String queryLocation="http://localhost:8080/SNEE-WS/services/QueryInterface?wsdl";
		adapter.addPullSource("http://localhost:8080/CCO/services/QueryInterface?wsdl", SourceType.SERVICE);
		//adapter.getResourceList(queryLocation);
	}

	@Test@Ignore
	public void testGetPullResourceListCCO() throws QueryException
	{
		
		//adapter.getPropertyDocument("envdata_boscombe", "http://localhost:8282/MockCCOWS/services/PullStream?wsdl");
		String queryLocation="http://localhost:8080/EMU/services/PullStream?wsdl";
		adapter.getPullResourceList(queryLocation);
		adapter.pullData2("emu:pull:Chesil_wave", true, 20, queryLocation);
		
	}
	
	

	
	@Test@Ignore
	public void testGetPropertyDocument()
	{
		
		//String queryLocation="http://localhost:8080/SNEE-WS-0.2.1/services/QueryInterface?wsdl";
		String queryLocation="http://localhost:8080/SNEE-WS/services/QueryInterface?wsdl";
		adapter.getPropertyDocument("snee:query", queryLocation);
	}
	
	@Test@Ignore
	public void testQueryFactory()
	{
		
		//String query ="SELECT Lon,Hs,timestamp,Lat FROM envdata_milford [FROM NOW-10 MINUTES TO NOW SLIDE 30 SECONDS]";
		//String query ="SELECT * FROM envdata_milford [FROM NOW-10 MINUTES TO NOW SLIDE 30 SECONDS];";
		//String query ="SELECT intAttr FROM BullStream;";
		//"SELECT wav FROM ((SELECT Hs as wav FROM envdata_milford) AS wavs);";
		//String query = "(SELECT Lon as lon,Hs as waveheight,timestamp as ts,Lat as lat FROM envdata_milford) UNION " +
		//"(SELECT Lon as lon,Hs as waveheight,timestamp as ts,Lat as lat FROM envdata_perranporth) UNION " +
		//"(SELECT Lon as lon,Hs as waveheight,timestamp as ts,Lat as lat FROM envdata_pevenseybay); ";
		String query = "(SELECT Hs AS waveheight,timestamp AS wavets,Lat AS lat FROM envdata_milford) UNION (SELECT Hs AS waveheight,timestamp AS wavets,Lat AS lat FROM envdata_folkestone);"; 

		String queryLocation="http://localhost:8282/SNEE-WS/services/QueryInterface?wsdl";
		//String queryLocation="http://localhost:8080/SNEE-WS-0.2.1/services/QueryInterface?wsdl";
		//String queryLocation="http://ssg4env.techideas.net:8088/SNEE-WS/services/QueryInterface?wsdl";
		String id = adapter.invokeQueryFactory(query, queryLocation);
		
		System.out.println("queryid: "+id);
	}

	@Test@Ignore
	public void testPullData() throws QueryException
	{
		String queryId ="pull:query1";
		String pullLocation = "http://localhost:8282/SNEE-WS/services/PullStream?wsdl";
		List<ResultSet> wrs = adapter.pullData(queryId);
        System.out.println("lolo "+ wrs.size());
	}

}
