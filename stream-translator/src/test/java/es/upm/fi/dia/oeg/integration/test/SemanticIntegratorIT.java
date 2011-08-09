package es.upm.fi.dia.oeg.integration.test;


import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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
import es.upm.fi.dia.oeg.integration.QueryExecutor;
import es.upm.fi.dia.oeg.integration.ResponseDocument;
import es.upm.fi.dia.oeg.integration.adapter.snee.test.SNEEAdapterTest;
import es.upm.fi.dia.oeg.integration.metadata.DataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.IntegratedDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.MappingDocumentMetadata;
import es.upm.fi.dia.oeg.integration.metadata.PullDataSourceMetadata;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;
import es.upm.fi.dia.oeg.integration.metadata.mappings.MappingLanguage;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistry;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.sparqlstream.test.SparqlStreamParseTest;

public class SemanticIntegratorIT extends SemanticIntegratorTest
{
	
	//static String queryConstructSimple = "";
	//static String querySimple = "";
	private static ConstantRatePushStreamGenerator generator;

	@BeforeClass
	public static void setUpBefore() throws IOException, URISyntaxException, SNEEConfigurationException, TypeMappingException, MetadataException, SchemaMetadataException, UnsupportedAttributeTypeException, SourceMetadataException, TopologyReaderException, SNEEDataSourceException, CostParametersException, SNCBException
	{
		init();
		//queryConstructSimple = ParameterUtils.loadAsString(SemanticIntegratorIT.class.getClassLoader().getResource("queries/testConstructSimple.sparql"));
		//querySimple = ParameterUtils.loadAsString(SemanticIntegratorIT.class.getClassLoader().getResource("queries/testQuerySimple.sparql"));
		
		logger.debug("Integrator uribase: "+ props.getProperty(IntegratorRegistry.INTEGRATOR_REPOSITORY_URL));
		//setUpRepository();
		
		Properties props = ParameterUtils.load(SNEEAdapterTest.class.getClassLoader().getResourceAsStream("snee/generator/snee.properties"));
		SNEEProperties.initialise(props );

		generator = new ConstantRatePushStreamGenerator();
		generator.startTransmission();

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		generator.stopTransmission();
	}

	//Memory tests sufficient 
	@Deprecated
	private static void setUpRepository() throws IOException 
	{
		FileOutputStream fos = new FileOutputStream( new URL(props.getProperty(IntegratorRegistry.INTEGRATOR_REPOSITORY_URL)+"mappings.xml").getPath());
		IOUtils.copy(SemanticIntegratorIT.class.getClassLoader().getResourceAsStream("mappings/mappings.xml"), fos);
		fos.close();
		fos = new FileOutputStream( new URL(props.getProperty(IntegratorRegistry.INTEGRATOR_REPOSITORY_URL)+"sources.xml").getPath());
		IOUtils.copy(SemanticIntegratorIT.class.getClassLoader().getResourceAsStream("mappings/sources.xml"), fos);
		fos.close();
	}

	@Test@Ignore	
	public void testIntegrateAs() throws URISyntaxException, InvalidR2RDocumentException, DataSourceException 
	{
		List<DataSourceMetadata> ss = new ArrayList<DataSourceMetadata>();
		DataSourceMetadata dataresource = new DataSourceMetadata("urn:name:sourceXXX",
				SourceType.SERVICE,new URI("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl"));
		//http://webgis1.geodata.soton.ac.uk:8080/WaveNet/services
		ss.add(dataresource);
		                     
		MappingDocumentMetadata mappingMD = new MappingDocumentMetadata("mappingTest",MappingLanguage.R2RML,null);
		mappingMD.setMapping(loadTestMappingModel());
		String isName = "urn:ssg4e:FolkstoneSourceSN";
		si.integrateAs(ss,  isName, mappingMD);
		
	}
	

	@Test@Ignore
	
	public void testGetPropertyDocument() throws DataSourceException, IntegratorRegistryException
	{
		IntegratedDataSourceMetadata intMD = si.retrieveIntegratedDataSource("urn:ssg4e:FolkstoneSourceSN");
		logger.info(intMD.getServiceDescription().getDocument());
		
	}
	
	
	@Test
	public void testPullConstructSimpleQueryFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;
		
			//dataResourceName = "http://semsorgrid4env.eu/integratedSources/hernebay_metIDS";
			dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
			//QueryExecutor qe = QueryExecutor.getInstance(props);
			//qe.addSource("file:wsdl/pull_stream_service.wsdl");
		QueryDocument queryDoc = new QueryDocument(testConstructSimple);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
Thread.sleep(10000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		
		resp.getRdfResultSet().write(System.out);
		
		//printSparqlResult(resp.getResultSet());
		
		
		}


	@Test
	public void testPullConstructQueryFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument(testConstruct);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		
		resp.getRdfResultSet().write(System.out);
				
	}

	@Test
	public void testPullConstructTideQueryFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument(testConstructTide);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		
		resp.getRdfResultSet().write(System.out);
				
	}

	@Test
	public void testPullQueryFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument(testQuery);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
	
		
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		Utils.printSparqlResult(resp.getResultSet());
		/*
		Thread.sleep(10000);
		resp = si.pullData(pullMD.getSourceName());
		printSparqlResult(resp.getResultSet());
		*/
		
		
		
		
	}

	@Test
	public void testPullJoin() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument(testQueryJoin);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
					
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		Utils.printSparqlResult(resp.getResultSet());
		
	}

	@Test
	public void testPullConstructJoin() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument(testConstructJoin);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
					
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		resp.getRdfResultSet().write(System.out);
		
	}

	
	@Test
	public void testPullQuerySimpleFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorWave";
		QueryDocument queryDoc = new QueryDocument(testQuerySimple);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
	
		
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		Utils.printSparqlResult(resp.getResultSet());
		
		/*
		Thread.sleep(10000);
		resp = si.pullData(pullMD.getSourceName());
		printSparqlResult(resp.getResultSet());
		*/
		
		
		
		
	}

	@Test
	public void testPullQueryCCOFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorCCOWave";
		QueryDocument queryDoc = new QueryDocument(queryCCOWaveHeight);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
	
		
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		Utils.printSparqlResult(resp.getResultSet());
		/*
		Thread.sleep(10000);
		resp = si.pullData(pullMD.getSourceName());
		printSparqlResult(resp.getResultSet());
		
		*/
		
		
		
	}

	@Test
	public void testPullConstructCCOFactory() throws IntegratorRegistryException, QueryException, InterruptedException, InvalidR2RDocumentException, InvalidR2RLocationException, DataSourceException, IntegratorConfigurationException
	{
		String dataResourceName;

		dataResourceName = "urn:ssg4e:iqs:GeneratorCCOWave";
		QueryDocument queryDoc = new QueryDocument(constructCCOWaveHeight);
		logger.info(queryDoc.getQueryString());
		PullDataSourceMetadata pullMD =  si.pullQueryFactory(dataResourceName, queryDoc );
		logger.info("Query identifier:"+ pullMD.getQueryId()+" - "+pullMD.getSourceName());
		
	
		
		Thread.sleep(5000);
		
		ResponseDocument resp = null;
		
		resp = si.pullData(pullMD.getSourceName());
		resp.getRdfResultSet().write(System.out);
		
		
	}

	
	@Test@Ignore
	public void testRemoveIntegratedDataSource() throws URISyntaxException, DataSourceException, IntegratorRegistryException
	{
		si.removeIntegratedSource("urn:ssg4e:FolkstoneSourceSN" );
	}
	
		
}
