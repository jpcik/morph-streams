package es.upm.fi.oeg.integration.adapter.pachube;

import static es.upm.fi.dia.oeg.common.ParameterUtils.loadQuery;

import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.common.Utils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryDocument;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.ResponseDocument;
import es.upm.fi.dia.oeg.integration.SemanticIntegrator;
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;


public class PachubeQueryTest 
{
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
	public void testExecute() throws IntegratorRegistryException, IntegratorConfigurationException, QueryCompilerException, DataSourceException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException
	{
		SemanticIntegrator si = new SemanticIntegrator(props);
		QueryDocument queryDoc = new QueryDocument(queryTemp);
		ResponseDocument res = si.query("urn:oeg:PachubeTest", queryDoc );
		Utils.printSparqlResult(res.getResultSet());
	}
}
