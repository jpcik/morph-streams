package es.upm.fi.dia.oeg.integration.adapter.gsn.test;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;


import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3.sparql.results.Sparql;

import com.hp.hpl.jena.rdf.model.Model;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.QueryExecutor;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.adapter.gsn.GsnAdapter;
import es.upm.fi.dia.oeg.integration.adapter.gsn.GsnQuery;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.sparqlstream.SparqlUtils;
import es.upm.fi.dia.oeg.sparqlstream.StreamQuery;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;

public class GsnAdapterIT extends QueryTestBase 
{
	protected static Logger logger = Logger.getLogger(GsnAdapterIT.class.getName());
	static GsnAdapter gsn;
	static Properties props;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		//PropertyConfigurator.configure(SemanticIntegratorTest.class.getClassLoader().getResource("config/log4j.properties"));
		props = ParameterUtils.load(GsnAdapterIT.class.getClassLoader().getResourceAsStream("config/config_memoryStore.gsn.properties"));
		init();
		gsn = new GsnAdapter();
	}

	@Test
	public void testAddPullSource()
	{
		//fail("Not yet implemented");
	}

	@Test
	public void testInit() throws StreamAdapterException, QueryCompilerException, QueryException
	{
		gsn.init(props);
	}

	@Test//@Ignore
	public void testInvokeQuery() throws StreamAdapterException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException
	{
		QueryTranslator trans = new QueryTranslator(props);
		SourceQuery s= trans.translate(queryWannengratTemp, new URI("mappings/wannengrat.r2r"));
		GsnQuery gQuery = (GsnQuery)s;
		gsn.init(props);
		
		List<ResultSet> rs = gsn.invokeQuery(gQuery);
		logger.debug(rs.size());
		
	}

	
	@Test//@Ignore
	public void testInvokeWindowQuery() throws StreamAdapterException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException
	{
		QueryTranslator trans = new QueryTranslator(props);
		SourceQuery s= trans.translate(queryWannengratMetadataTemp, new URI("mappings/wannengrat.r2r"));
		GsnQuery gQuery = (GsnQuery)s;
		gsn.init(props);
		assertNotNull(gQuery.getWindow());
		assertEquals(1, gQuery.getWindow().getFromOffset());
		List<ResultSet> rs = gsn.invokeQuery(gQuery);
		logger.debug(rs.size());
		
	}

	
	@Test//@Ignore
	public void testTranslateInvokeQuery() throws StreamAdapterException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException, IntegratorConfigurationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		SourceQuery s= trans.translate(queryWannengratTemp, new URI("mappings/wannengrat.r2r"));
		GsnQuery gQuery = (GsnQuery)s;
		//gsn.init(props);
		QueryExecutor exe = new QueryExecutor(props);
		Sparql sparqlResult =exe.query(gQuery,QueryTranslator.getProjectList(queryWannengratTemp));
		//List<ResultSet> rs = gsn.invokeQuery(gQuery);
		
		logger.debug(sparqlResult.getResults().getResult().size());
		printSparqlResult(sparqlResult);
	}

	@Test//@Ignore
	public void testTranslateInvokeConstruct() throws StreamAdapterException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException, IntegratorConfigurationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		StreamQuery q = (StreamQuery) StreamQueryFactory.create(constructWannengratTemp);
		SourceQuery s= trans.translate(constructWannengratTemp, new URI("mappings/wannengrat.r2r"));
		GsnQuery gQuery = (GsnQuery)s;
		//gsn.init(props);
		QueryExecutor exe = new QueryExecutor(props);
		Model rdf =exe.query(gQuery,q.getConstructTemplate());
		//List<ResultSet> rs = gsn.invokeQuery(gQuery);
		
		
		rdf.write(System.out);
	}

	@Test//@Ignore
	public void testInvokeMetadataQuery() throws StreamAdapterException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException, IntegratorConfigurationException
	{		
		//props.put(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED, "true");
		QueryTranslator trans = new QueryTranslator(props);
		QueryExecutor exe = new QueryExecutor(props);
		long ini1 = System.currentTimeMillis();
		SourceQuery s= trans.translate(queryWannengratMetadataTemp, new URI("mappings/wannengrat1.r2r"));
		long span0 =(System.currentTimeMillis()-ini1);

		//QueryTranslator trans = new QueryTranslator(props,"http://localhost:8080/openrdf-workbench/repositories/wannengrat/query");
		double avg=0;
		//for (int i =0;i<6;i++)
		{
		long ini = System.currentTimeMillis();
		long span =(System.currentTimeMillis()-ini);
		GsnQuery gQuery = (GsnQuery)s;
		Sparql sparqlResult =exe.query(gQuery,trans.getProjectList(queryWannengratMetadataTemp));
		long span1 =(System.currentTimeMillis()-ini);
		printSparqlResult(sparqlResult);

		System.out.println(span0+"--"+span1);
		//if (i!=0)
			//avg+=span1;
	}
		System.out.println(avg/5);
	}

	@Test@Ignore
	public void testInvokeMetadataConstruct() throws StreamAdapterException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException, IntegratorConfigurationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		StreamQuery q = (StreamQuery) StreamQueryFactory.create(constructWannengratMetadataTemp);
		SourceQuery s= trans.translate(constructWannengratMetadataTemp, new URI("mappings/wannengrat.r2r"));
		GsnQuery gQuery = (GsnQuery)s;
		gsn.init(props);
		
		List<ResultSet> rs = gsn.invokeQuery(gQuery);
		logger.debug(rs.size());

		QueryExecutor exe = new QueryExecutor(props);
		Model rdf =exe.query(gQuery,q.getConstructTemplate());
		rdf.write(System.out);


	}
	@Test
	public void testInvokeQueryFactory()
	{
		//fail("Not yet implemented");
	}

	@Test
	public void testPullData()
	{
		//fail("Not yet implemented");
	}

}
