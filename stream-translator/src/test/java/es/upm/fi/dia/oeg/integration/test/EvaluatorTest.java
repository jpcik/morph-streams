package es.upm.fi.dia.oeg.integration.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;

import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.common.Utils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryDocument;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.ResponseDocument;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;

public class EvaluatorTest extends QueryTestBase
{
	private static ConstantRatePushStreamGenerator generator;
	private static SemanticIntegrator si;
	private static SNEEAdapter snee;
	private static QueryTranslator qt; 

	//private static String query1SensorSimple = loadQuery("queries/evaluate/query1SensorSimple.sparql");
	private static String querySingleSensorSimple = loadQuery("queries/evaluate/querySingleSensorSimple.sparql");
	private static String query2SensorJoin = loadQuery("queries/evaluate/query2SensorJoin.sparql");
	
	
	private static String loadQuery(String path)
	{
		try {
			return ParameterUtils.loadAsString(EvaluatorTest.class.getClassLoader().getResource(path));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;			
	}
	
	@BeforeClass
	public static void before() throws IOException, SNEEConfigurationException, 
		IntegratorRegistryException, IntegratorConfigurationException, TypeMappingException, MetadataException, 
		SchemaMetadataException, UnsupportedAttributeTypeException, SourceMetadataException, TopologyReaderException, 
		SNEEDataSourceException, CostParametersException, SNCBException, StreamAdapterException
	{
		PropertyConfigurator.configure(QueryTestBase.class.getClassLoader().getResource("config/log4j.properties"));

		Properties props = ParameterUtils.load(EvaluatorTest.class.getClassLoader().getResourceAsStream("snee/evaluate/snee.properties"));
		SNEEProperties.initialise(props );
		generator = new ConstantRatePushStreamGenerator();
		generator.startTransmission();
		snee = new SNEEAdapter();
		props = ParameterUtils.load(EvaluatorTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.evaluate.properties"));		
		snee.init(props);
		si = new SemanticIntegrator(props);
		qt = new QueryTranslator(props);
		//qt.
	
		
	}
	
	@AfterClass
	public static void after()
	{
		generator.stopTransmission();
	}
	
	@Test@Ignore
	public void testIncreasing() throws InterruptedException, DataSourceException, QueryException
	{
		
		PullDataSourceMetadata pullMD =  
			si.pullQueryFactory("urn:ssg4e:iqs:Evaluator", 
								new QueryDocument(querySingleSensorSimple));
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());					
		for (int i=0;i<3000;i++)
		{
			Thread.sleep(10);
			ResponseDocument resp = si.pullNewestData(pullMD.getSourceName(),5);
			Utils.printSparqlResult(resp.getResultSet());
		}
		
		
	}


	private void call(Collection<PullDataSourceMetadata> pullList)
	{
		long tot =0;
		int iter = 20;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Collection<Caller> callers = Lists.newArrayList();
		for (int i=0;i<iter;i++)
		{
			Caller cal = new Caller();
			cal.si = EvaluatorTest.si;
			cal.pullList = pullList;
			callers.add(cal);
		}
		
		for (Caller cal:callers)
		{
			cal.start();
		}
		boolean allfinished = false;
		
		while (!allfinished)
		{
			int count =0;
			for (Caller cal:callers)
			{
				if (!cal.finished && cal.isAlive())
					break;
				else 
					count++;
				
			}
			if (count==callers.size())
				allfinished=true;
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Note yet: "+count);
			
		}
		
		int size = callers.size();
		for (Caller cal:callers)
		{
			if (cal.finished)
			{
			tot+=cal.avg;
			System.out.println(cal.avg);
			}
			else
				size--;
		}
		
		logger.info("time avg: "+(tot/size));

	}

	@Test@Ignore
	public void testTranslate() throws QueryException, SQLException, InterruptedException, DataSourceException
	{
		//String id =snee.invokeQueryFactory("(SELECT Hs AS waveheight, DateTime AS wavets FROM envdata_milford) ;", 5);
		Set<String> ids= Sets.newHashSet();
		for (int j=0;j<50;j++)
			ids.add(snee.invokeQueryFactory("(SELECT Hs AS waveheight, DateTime AS wavets FROM envdata_milford) ;", 5));
		//PullDataSourceMetadata pullMD =  si.pullQueryFactory("urn:ssg4e:iqs:Evaluator", 
		//		new QueryDocument(query1SensorSimple));

		Collection<Double> avgs = Lists.newArrayList();
		//Thread.sleep(10000);
		for (int i=0;i<20;i++)
		{
			long total = 0;
				Thread.sleep(50);
for (String id:ids)
{
		long ini = System.currentTimeMillis();
		//ResponseDocument resp = si.pullNewestData(pullMD.getSourceName());
		//Utils.printSparqlResult(resp.getResultSet());
		List<ResultSet> result = snee.pullNewestData(id);
		if (result != null)
			SNEEAdapter.printResults(result);
		long elapsed =System.currentTimeMillis()-ini ;
		total+=elapsed;
}
avgs.add((double)total/ids.size());

		//System.out.println(elapsed);
		}
		
		for (Double l:avgs)
			System.out.println(""+l);
	}

	@Test@Ignore
	public void testTranslateQuery() throws QueryException, SQLException, InterruptedException, DataSourceException, URISyntaxException
	{
		String id =snee.invokeQueryFactory("(SELECT Hs AS waveheight, DateTime AS wavets FROM envdata_milford) ;", 5);
		//PullDataSourceMetadata pullMD =  si.pullQueryFactory("urn:ssg4e:iqs:Evaluator", 
		//		new QueryDocument(query1SensorSimple));
		String query = querySingleSensorSimple;
		Collection<Long> avgs = Lists.newArrayList();
		//Thread.sleep(5000);
		qt.translate(query, new URI("mappings/ssg4env.ttl"));

		for (int i=0;i<30;i++)
		{
			Thread.sleep(100);

		long ini = System.currentTimeMillis();
		qt.translate(query, null);
		long elapsed =System.currentTimeMillis()-ini ;
		avgs.add(elapsed);
		}
		
		for (Long l:avgs)
			System.out.println(""+l);
	}

	
	@Test//@Ignore
	public void testIncreasingQueries() throws InterruptedException, DataSourceException, QueryException
	{
		int quer = 1;
				
		Collection<PullDataSourceMetadata> pullList = Lists.newArrayList();
		for (int k=0;k<quer;k++)
		{
			
			PullDataSourceMetadata pullMD =  si.pullQueryFactory("urn:ssg4e:iqs:Evaluator", 
									new QueryDocument(querySingleSensorSimple));
			logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());					
			pullList.add(pullMD);
		}
		call(pullList);
	}

	@Test@Ignore
	public void testIncreasingJoinQueries() throws InterruptedException, DataSourceException, QueryException
	{
		int quer = 1;
		
		
		Collection<PullDataSourceMetadata> pullList = Lists.newArrayList();
		for (int k=0;k<quer;k++)
		{
			
			PullDataSourceMetadata pullMD =  si.pullQueryFactory("urn:ssg4e:iqs:Evaluator", 
									new QueryDocument(query2SensorJoin));
			logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());					
			pullList.add(pullMD);
		}
		call(pullList);
	}
}

class Caller extends Thread
{
	public Collection<PullDataSourceMetadata> pullList;
	public SemanticIntegrator si;
	public double avg;
	public boolean finished = false;
	@Override
	public void run()
	{
		int toti =0;
		//int iter = 5;
		try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//for (int k =0;k<iter;k++)
		for (PullDataSourceMetadata pullMD:pullList)
		{
			//logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());					
			long ini = System.currentTimeMillis();
			ResponseDocument resp;
			try {
				resp = si.pullNewestData(pullMD.getSourceName(),1000);
				long elapsed =System.currentTimeMillis()-ini ;
				//Utils.printSparqlResult(resp.getResultSet());
				System.out.println(elapsed);
				toti += elapsed;
			} catch (DataSourceException e) {
				e.printStackTrace();

			} catch (QueryException e) {
				e.printStackTrace();
			}
		}
		avg= toti/(pullList.size());
		finished= true;
	}
}
