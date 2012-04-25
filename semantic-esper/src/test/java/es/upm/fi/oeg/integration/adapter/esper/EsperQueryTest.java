package es.upm.fi.oeg.integration.adapter.esper;

import static es.upm.fi.dia.oeg.common.ParameterUtils.loadQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.espertech.esper.client.Configuration;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.common.Utils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryDocument;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.ResponseDocument;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.Statement;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.sparqlstream.SparqlUtils;
import es.upm.fi.oeg.integration.adapter.esper.model.Stream;


public class EsperQueryTest 
{
	 static Properties props;
	private static Logger logger = Logger.getLogger(EsperQueryTest.class.getName());
	private static Logger timingLog = Logger.getLogger(EsperQueryTest.class.getName()+"Timing");

	private static SemanticIntegrator si;
	PullDataSourceMetadata md;
	protected static String queryTemp=loadQuery("queries/esper/queryTemp.sparql");
	protected static String query2SensorJoin=loadQuery("queries/evaluate/query2SensorJoin.sparql");
	protected static String querySensorUnion=loadQuery("queries/evaluate/querySensorUnion.sparql");
	protected static String querySingleSensorSimple=loadQuery("queries/evaluate/querySingleSensorSimple.sparql");
	static String[] extents = new String[]{"envdata_milford",
			"envdata_perranporth", 
			"envdata_pevenseybay",
			"envdata_goodwin",
			"envdata_torbay",
			"envdata_rustington",
			"envdata_bidefordbay",
			"envdata_folkestone",
			"envdata_boscombe",
			"envdata_penzance",
			"envdata_weymouth",
			"envdata_rye",
			"envdata_westonbay",
			"envdata_haylingisland",	
			"envdata_hornsea",
			"envdata_rhylflats",
			"envdata_chesil",
			"envdata_westbay",
			"envdata_looebay",
			"envdata_startbay",
			"envdata_sandownbay",
			"envdata_minehead",
			"envdata_seaford",
			"envdata_bracklesham",
			"envdata_lymington_tide",
			"envdata_hernebay_tide",
			"envdata_deal_tide",
			"envdata_teignmouthpier_tide",
			"envdata_swanagepier_tide",
			"envdata_sandownpier_tide",
			"envdata_westbaypier_tide",
			"envdata_deal_met",
			"envdata_hernebay_met",
			"envdata_looebay_met",
			"envdata_arunplatform_met", 
			"envdata_swanagepier_met", 
			"envdata_sandownpier_met", 
			"envdata_weymouth_met", 
			"envdata_westbaypier_met", 
			"envdata_teignmouthpier_met", 
			"envdata_folkestone_met", 
			"envdata_lymington_met", 
			"envdata_worthing_met"};
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception 
	{
		PropertyConfigurator.configure(
				EsperQueryTest.class.getClassLoader().getResource("config/log4j.properties"));
		props = ParameterUtils.load(
				EsperQueryTest.class.getClassLoader().getResourceAsStream(
						"config/config_memoryStore.esper.properties"));
		
		Configuration configuration = new Configuration();
        configuration.addEventType("Stream", Stream.class);
        for (String ext:extents)
        {
	        Map<String,Object> map = Maps.newHashMap();
	        map.put("DateTime", Long.class);
	        map.put("Hs", Double.class);
	        map.put("timestamp", Long.class);        
	        configuration.addEventType(ext, map);
        }
        /*
        Map<String,Object> map1 = Maps.newHashMap();
        map1.put("DateTime", Long.class);
        map1.put("Tp", Double.class);
        map1.put("timestamp", Long.class);        
        configuration.addEventType("envdata_hernebay_tide", map1);*/
        props.put("configuration", configuration);
		si = new SemanticIntegrator(props);
		
		final EsperAdapter esper = (EsperAdapter)si.getExecutor().getAdapter();
		final int rate = 100; 
		
		
		Thread t = new Thread(new Runnable() {
			
			Random r = new Random();	
			@Override
			public void run() {
				while (true)
				{
				logger.trace("sending ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
				for (String extent:extents)
				{
				Map<String,Object> map = Maps.newHashMap();
				long time = r.nextLong();
				map.put("DateTime", r.nextLong());
				map.put("Hs", r.nextDouble());
				map.put("timestamp", time);
				esper.sendEvent(map, extent);
				}

				//esper.sendEvent(new Stream(r.nextDouble()));
				try {
					int milis = (int)(1000/rate);
					int nanos = (1000%rate)*1000;
					//System.out.println(milis+"- "+nanos);
					//System.exit(0);
					if (nanos>=1000000) nanos = 1;
					

					Thread.sleep(milis,nanos);
					//System.exit(0);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				}
			}
		});
	
		t.start();
		
		
		
	}
	
	@Test@Ignore
	public void testExecute() throws IntegratorRegistryException, IntegratorConfigurationException, QueryCompilerException, DataSourceException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException
	{
		//SemanticIntegrator si = new SemanticIntegrator(props);
		QueryDocument queryDoc = new QueryDocument(queryTemp);		
		Statement res = si.registerQuery("urn:oeg:EsperTest", queryDoc );
		EsperListener listener = new  EsperListener();
		EsperStatement sta = (EsperStatement)res;
		sta.addListener(listener);
		EsperAdapter esper = (EsperAdapter)si.getExecutor().getAdapter();
		esper.sendEvent(new Stream(23.5));
		
		esper.sendEvent(new Stream(28.5));								
	}
	
	@Test@Ignore
	public void testPullNewestData() throws DataSourceException, QueryException, IntegratorRegistryException, IntegratorConfigurationException, InterruptedException
	{
		QueryDocument queryDoc = new QueryDocument(queryTemp);
		//PullDataSourceMetadata md = si.pullQueryFactory("urn:oeg:EsperTest", queryDoc);
		EsperAdapter esper = (EsperAdapter)si.getExecutor().getAdapter();
		String key = esper.invokeQueryFactory("Select m1.windspeed from envdata_milford m1, envdata_milford m2 where m1.timestamp=m2.timestamp;", 0);
		Thread.sleep(10000);
		List<ResultSet> rs =  esper.pullNewestData(key);	

		long before = System.currentTimeMillis();
		//ResponseDocument response = si.pullNewestData(md.getSourceName());
		//System.out.println("elapsed:"+(System.currentTimeMillis()-before));
		//System.out.println(SparqlUtils.print(response.getResultSet()));
	}
	
	@Test@Ignore
	public void testPullSimple() throws DataSourceException, QueryException, InterruptedException
	{
		QueryDocument queryDoc = new QueryDocument(querySingleSensorSimple);
		md = si.pullQueryFactory("urn:oeg:EsperEvaluation", queryDoc);
		
		
		//Thread.sleep(10000);
		Timer t = new Timer(true);
		t.scheduleAtFixedRate(new TimerTask() {			
			@Override
			public void run() {
				if (si == null || md == null)
					return;
				long before = System.currentTimeMillis();	
				ResponseDocument res;
				try {/*
					EsperAdapter loc = (EsperAdapter)si.getExecutor().getAdapter();
					List<ResultSet> list = loc.pullNewestData(md.getQueryId());
					for (ResultSet rs:list)
						rs.close();*/
					res = si.pullNewestData(md.getSourceName());
					timingLog.debug("elapsed:"+(System.currentTimeMillis()-before));
					//logger.info(SparqlUtils.print(res.getResultSet()));		
				} catch (DataSourceException e) {					
					e.printStackTrace();
				} catch (QueryException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				/*} catch (SQLException e) {
					e.printStackTrace();*/
				}				
			}
		}, 0L, 5000);
		//while (true)
		{
			Thread.sleep(5000);
		}
	}
	
	@Test//@Ignore
	public void testPushJoin() throws DataSourceException, QueryException, InterruptedException
	{
		QueryDocument queryDoc = new QueryDocument(querySingleSensorSimple);
		EsperStatement md = (EsperStatement)si.registerQuery("urn:oeg:EsperEvaluation", queryDoc);
		md.addListener(new EsperListener());
		
		Thread.sleep(5000);		
	}

	@Test@Ignore
	public void testPushUnion() throws DataSourceException, QueryException, InterruptedException
	{
		//Esper does not support UNION
		QueryDocument queryDoc = new QueryDocument(querySensorUnion);
		EsperStatement md = (EsperStatement)si.registerQuery("urn:oeg:EsperEvaluation", queryDoc);
		md.addListener(new EsperListener());
		
		Thread.sleep(5000);		
	}

}
