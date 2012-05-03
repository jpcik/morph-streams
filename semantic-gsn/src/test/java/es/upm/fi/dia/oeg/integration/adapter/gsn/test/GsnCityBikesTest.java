package es.upm.fi.dia.oeg.integration.adapter.gsn.test;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3.sparql.results.Sparql;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.common.Utils;
import es.upm.fi.dia.oeg.integration.IntegratorConfigurationException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.QueryExecutor;
import es.upm.fi.dia.oeg.integration.SourceQuery;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.dia.oeg.integration.adapter.gsn.GsnAdapter;
import es.upm.fi.dia.oeg.integration.adapter.gsn.GsnQuery;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslationException;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import static es.upm.fi.dia.oeg.common.ParameterUtils.*;


public class GsnCityBikesTest {
	protected static Logger logger = Logger.getLogger(GsnCityBikesTest.class.getName());
	static GsnAdapter gsn;
	static Properties props;
	static String queryBikes=loadQuery("queries/citybikes/queryBikes.sparql");
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		PropertyConfigurator.configure(GsnCityBikesTest.class.getClassLoader().getResource("config/log4j.properties"));
		props = ParameterUtils.load(GsnCityBikesTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.gsn.properties"));
		gsn = new GsnAdapter();
	}
	
	@Test@Ignore
	public void test() throws URISyntaxException, StreamAdapterException, QueryException, IntegratorConfigurationException {
		QueryTranslator trans = new QueryTranslator(props);
		SourceQuery s= trans.translate(queryBikes, new URI("mappings/citybikes.ttl"));
		GsnQuery gQuery = (GsnQuery)s;
		gsn.init(props);
		QueryExecutor exe = new QueryExecutor(props);
		Sparql sparqlResult =exe.query(gQuery,QueryTranslator.getProjectList(queryBikes));
		//List<ResultSet> rs = gsn.invokeQuery(gQuery);
		
		logger.debug(sparqlResult.getResults().getResult().size());
		Utils.printSparqlResult(sparqlResult);	
		//List<ResultSet> rs = gsn.invokeQuery(gQuery);	
	}

}
