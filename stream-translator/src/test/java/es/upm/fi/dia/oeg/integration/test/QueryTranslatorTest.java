package es.upm.fi.dia.oeg.integration.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslationException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;

public class QueryTranslatorTest extends QueryTestBase
{

	private static Logger logger = Logger.getLogger(QueryTranslatorTest.class.getName());
	static Properties props;
	
	@BeforeClass
	public static void setUp() throws IOException, URISyntaxException
	{
		init();
		props = ParameterUtils.load(QueryTranslatorTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.properties"));
	}
	
	@Test
	public void testTranslate() throws QueryException, InvalidR2RDocumentException, InvalidR2RLocationException, URISyntaxException {
		logger.info(queryCCOComplexTide);
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryCCOComplexTide, new URI("mappings/cco.r2r"));	}

	
	@Test 
	public void testAlgebraTransformation() throws  QueryTranslationException, URISyntaxException
	{
		
		logger.info(queryCCO);
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryCCO, new URI("mappings/cco.r2r"));
}
	
	@Test
	public void testParseNoStreams()
	{
		String queryString = "PREFIX fire: <http://www.semsorgrid4env.eu#> \n"+
							
		" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"+ 
		" SELECT ((?sid + ?sid * ?speed) AS ?tot) ?speed (seconds(?speed) AS ?popo) \n"+  
		//" SELECT TIMESTAMP(?sid) AS ?pipo "+  
		//" SELECT (count(*) AS ?coso) ?direction ?sid ?name"+
		" WHERE"+ 
		" {"+ 
		" ?WindSpeed a fire:WindSpeedMeasurement;"+ 
		" fire:hasSpeed ?speed;"+ 
		" fire:isProducedBy ?Sensor."+ 
		" ?WindDirection a fire:WindDirectionMeasurement;"+ 
		" fire:hasDirection ?direction."+  
		" ?Sensor a fire:Sensor;"+  
		" fire:hasName ?name;"+ 
		" fire:hasSensorid ?sid."+ 
		" FILTER ( ?speed > 4 && ?speed <10)"+ 
		" FILTER ( ?speed < 10 )"+ 
		" FILTER ( ?direction = 3 )"+ 
		" }";   
		
		Query query = StreamQueryFactory.create(queryString);
		System.out.println(query.toString());
				
		Op algebra = Algebra.compile(query);
		System.out.println(algebra);
	}

	
	@Test
	public void testParse()
	{
		String queryString = "PREFIX fire: <http://www.semsorgrid4env.eu#>"+							
		" PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+ 
		" SELECT ?speed ?direction ?sid ?name "+  
		" FROM NAMED STREAM <http://www.ssg4env/mes.srdf> [NOW SLIDE 2 HOURS]"+
		" FROM NAMED STREAM <http://www.ssg4env/mes1.srdf> [NOW - 10 HOURS SLIDE 1 MINUTES]"+
		" FROM NAMED STREAM <http://www.ssg4env/mes2.srdf>"+
		" [NOW - 10 HOURS TO NOW - 0 HOURS SLIDE 1 MINUTES]"+ 
		" WHERE"+ 
		" {"+ 
		" ?WindSpeed a fire:WindSpeedMeasurement;"+ 
		" fire:hasSpeed ?speed;"+ 
		" fire:isProducedBy ?Sensor."+ 
		" ?WindDirection a fire:WindDirectionMeasurement;"+ 
		" fire:hasDirection ?direction."+  
		" ?Sensor a fire:Sensor;"+  
		" fire:hasName ?name;"+ 
		" fire:hasSensorid ?sid."+ 
		" FILTER ( ?speed > 4 && ?speed < 12 && ?direction >40)"+ 
		" FILTER ( ?speed < 10 )"+ 
		" FILTER ( ?direction = 3 )"+ 
		" }";   
		
		Query query = StreamQueryFactory.create(queryString);
		System.out.println("tic:"+ query.toString());
		
	}
	
	@Test
	public void testTranslateQuery() throws  QueryTranslationException, URISyntaxException
	{
		logger.info(testQuery);
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testQuery, new URI("mappings/testMapping.r2r"));		
	}

	@Test
	public void testTranslateIsolatedQuery() throws  QueryTranslationException, URISyntaxException
	{
		logger.info(testQueryIsolate);
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testQueryIsolate, new URI("mappings/testMapping.r2r"));		
	}

	
	@Test
	public void testTranslateWithSQL() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testQuerySimple, new URI("mappings/testMappingSQL.r2r"));
	}

	@Test
	public void testTranslateConstructSimple() throws URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testConstructSimple, new URI("mappings/testMapping.r2r"));
	}

	@Test
	public void testTranslateConstruct() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testConstruct, new URI("mappings/testMapping.r2r"));
	}

	@Test
	public void testTranslateFilters() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testQueryFilter, new URI("mappings/testMapping.r2r"));
	}

	@Test
	public void testTranslateJoin() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(testQueryJoin, new URI("mappings/testMapping.r2r"));
	}

	
	@Test
	public void testTranslateCCOWaveHeight() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryCCOWaveHeight, new URI("mappings/cco.r2r"));
	}
	
	@Test
	public void testTranslateWannengratTemp() throws URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryWannengratTemp, new URI("mappings/wannengrat.r2r"));
	}

	@Test
	public void testTranslateWannengratMetadataTemp() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryWannengratMetadataTemp, new URI("mappings/wannengrat.r2r"));
	}

	@Test//@Ignore
	public void testTranslateTwoWaves() throws  URISyntaxException, QueryTranslationException, IOException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(loadString("queries/testQueryTwoWaves.sparql"), new URI("mappings/testMapping.r2r"));
	}

	@Test@Ignore
	public void testTranslateService() throws  URISyntaxException, QueryTranslationException, IOException
	{
		QueryTranslator trans = new QueryTranslator(props);
		OpInterface op = trans.translateToAlgebra(loadString("queries/testQueryService.sparql"), new URI("mappings/wannengrat1.r2r"));
		
	}

	
	@Test@Ignore
	public void testTranslateWannengratMetadataTempRemote() throws  QueryTranslationException
	{
		props.put(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED, "true");
		QueryTranslator trans = new QueryTranslator(props,"http://localhost:8080/openrdf-workbench/repositories/wannengrat/query");
		trans.translate(queryWannengratMetadataTempRemote, null);
	}

	
}
