package es.upm.fi.dia.oeg.integration.test;


import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryDocument;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.ResponseDocument;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;

public class SemanticIntegratorSSG4EnvIT extends QueryTestBase
{
	protected static SemanticIntegrator si;
	protected static Properties props;

	@BeforeClass
	public static void setUp() throws Exception
	{
		init();
		props = ParameterUtils.load(SemanticIntegratorTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.ssg4e.properties"));
		si = new SemanticIntegrator(props);

		//queryConstructSimple = ParameterUtils.loadAsString(SemanticIntegratorIT.class.getClassLoader().getResource("queries/testConstructSimple.sparql"));
		//querySimple = ParameterUtils.loadAsString(SemanticIntegratorIT.class.getClassLoader().getResource("queries/testQuerySimple.sparql"));
		
		logger.debug("Integrator uribase: "+ props.getProperty(IntegratorRegistry.INTEGRATOR_REPOSITORY_URL));
		//setUpRepository();
	}

	@Test@Ignore
	public void testPullQueryFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument();
		queryDoc.setQueryString(testQuery);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
	
		
		Thread.sleep(10000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		printSparqlResult(resp.getResultSet());
		/*
		Thread.sleep(10000);
		resp = si.pullData(pullMD.getSourceName());
		printSparqlResult(resp.getResultSet());
		*/
		
		
		
		
	}
}
