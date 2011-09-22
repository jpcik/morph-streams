package es.upm.fi.oeg.integration.adapter.pachube;


import static es.upm.fi.dia.oeg.common.ParameterUtils.loadQuery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslationException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;

public class PachubeQueryTranslationTest {

	static Properties props;
	protected static String queryTemp=loadQuery("queries/pachube/queryTemp.sparql");

	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception 
	{
		PropertyConfigurator.configure(
				PachubeQueryTranslationTest.class.getClassLoader().getResource("config/log4j.properties"));
		props = ParameterUtils.load(
				PachubeQueryTranslationTest.class.getClassLoader().getResourceAsStream(
						"config/config_memoryStore.pachube.properties"));
		
	}
	
	@Test
	public void testTranslate() throws QueryTranslationException, URISyntaxException
	{
		QueryTranslator trans = new QueryTranslator(props);
		trans.translate(queryTemp, new URI("mappings/pachube.ttl"));
	}

}
