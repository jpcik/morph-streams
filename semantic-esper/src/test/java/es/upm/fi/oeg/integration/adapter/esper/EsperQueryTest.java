package es.upm.fi.oeg.integration.adapter.esper;

import static es.upm.fi.dia.oeg.common.ParameterUtils.loadQuery;

import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;

import com.espertech.esper.client.Configuration;

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
import es.upm.fi.dia.oeg.integration.registry.IntegratorRegistryException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RDocumentException;
import es.upm.fi.dia.oeg.morph.r2rml.InvalidR2RLocationException;
import es.upm.fi.oeg.integration.adapter.esper.model.Stream;


public class EsperQueryTest 
{
	static Properties props;
	protected static String queryTemp=loadQuery("queries/esper/queryTemp.sparql");

	
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
        props.put("configuration", configuration);

		
	}
	
	@Test
	public void testExecute() throws IntegratorRegistryException, IntegratorConfigurationException, QueryCompilerException, DataSourceException, QueryException, InvalidR2RDocumentException, InvalidR2RLocationException
	{
		SemanticIntegrator si = new SemanticIntegrator(props);
		QueryDocument queryDoc = new QueryDocument(queryTemp);		
		Statement res = si.registerQuery("urn:oeg:EsperTest", queryDoc );
		EsperListener listener = new  EsperListener();
		EsperStatement sta = (EsperStatement)res;
		sta.addListener(listener);
		EsperAdapter esper = (EsperAdapter)si.getExecutor().getAdapter();
		esper.sendEvent(new Stream(23.5));
		
		esper.sendEvent(new Stream(28.5));
		
		
		
	}
}
