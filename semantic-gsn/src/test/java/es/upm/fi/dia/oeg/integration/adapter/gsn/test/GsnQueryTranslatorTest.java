package es.upm.fi.dia.oeg.integration.adapter.gsn.test;

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
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslationException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.dia.oeg.sparqlstream.StreamQueryFactory;

public class GsnQueryTranslatorTest  extends QueryTestBase
{

	private static Logger logger = Logger.getLogger(GsnQueryTranslatorTest.class.getName());
	static Properties props;
	
	@BeforeClass
	public static void setUp() throws IOException, URISyntaxException
	{
		init();
		props = ParameterUtils.load(GsnQueryTranslatorTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.gsn.properties"));
	}
	
	@Test@Ignore
	public void testTranslateWannengratMetadataTemp() throws  URISyntaxException, QueryTranslationException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryWannengratMetadataTemp, new URI("mappings/wannengrat.r2r"));
	}

	@Test@Ignore
	public void testTranslateWannengratMetadataTempRemote() throws  QueryTranslationException, URISyntaxException
	{
		props.put(SemanticIntegrator.INTEGRATOR_METADATA_MAPPINGS_ENABLED, "true");
		QueryTranslator trans = new QueryTranslator(props,"http://localhost:8080/openrdf-workbench/repositories/wannengrat/query");
		long init = System.currentTimeMillis();
		trans.translate(queryWannengratMetadataTempRemote,null);//, new URI("mappings/sensorMetadata.ttl"));
		long span = System.currentTimeMillis() - init;
		System.out.println(span);
	}

	
}
