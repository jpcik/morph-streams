package es.upm.fi.dia.oeg.integration.adapter.gsn.test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;


import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3.sparql.results.Sparql;

import es.upm.fi.dia.oeg.common.ParameterUtils;

public abstract class QueryTestBase
{
	protected static String queryCCO="";
	protected static String queryCCOComplex="";
	protected static String queryCCOComplexTide = "";
	protected static String testQuery="";
	protected static String testQuerySimple="";
	protected static String testQueryIsolate="";
	protected static String testQueryFilter="";
	protected static String testQueryJoin="";
	protected static String testConstructSimple="";
	protected static String testConstructJoin="";
	protected static String testConstruct="";
	protected static String testConstructTide="";
	protected static String testStreamGraphSimple="";
	
	protected static String queryCCOWaveHeight="";
	protected static String constructCCOWaveHeight="";
	protected static String queryWannengratTemp="";
	protected static String queryWannengratMetadataTemp="";
	protected static String queryWannengratMetadataTempRemote="";
	protected static String constructWannengratTemp="";
	protected static String constructWannengratMetadataTemp="";
	protected static Logger logger = Logger.getLogger(QueryTestBase.class.getName());

	protected static void init() throws URISyntaxException, IOException
	{
		PropertyConfigurator.configure(QueryTestBase.class.getClassLoader().getResource("config/log4j.properties"));

		queryCCO= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("mappings/cco_query.sparql"));
		queryCCOComplex= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("mappings/cco_queryComplex.sparql"));
		queryCCOComplexTide= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("mappings/cco_queryComplexWind.sparql"));
		testQuery= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testQuery.sparql"));
		testQuerySimple= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testQuerySimple.sparql"));
		testQueryFilter= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testQueryFilter.sparql"));
		testQueryJoin= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testQueryJoin.sparql"));
		testQueryIsolate= ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testQueryIsolate.sparql"));
		testConstructSimple = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testConstructSimple.sparql"));
		testConstructJoin = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testConstructJoin.sparql"));
		testConstruct = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testConstruct.sparql"));
		testConstructTide = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/testConstructTide.sparql"));
		testStreamGraphSimple = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/common/testStreamGraphSimple.sparql"));
		
		queryCCOWaveHeight = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/queryCCOWaveHeight.sparql"));		
		constructCCOWaveHeight = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/constructCCOWaveHeight.sparql"));
		queryWannengratTemp = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/wannengrat/queryTemp.sparql"));		
		queryWannengratMetadataTemp = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/wannengrat/queryMetadataTemp.sparql"));		
		queryWannengratMetadataTempRemote = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/wannengrat/queryMetadataTempRemote.sparql"));		
		constructWannengratTemp = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/wannengrat/constructTemp.sparql"));		
		constructWannengratMetadataTemp = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource("queries/wannengrat/constructMetadataTemp.sparql"));		

	}
	
	public String loadString(String path) throws IOException, URISyntaxException
	{
		String val = ParameterUtils.loadAsString(QueryTestBase.class.getClassLoader().getResource(path));
		return val;
	}

	protected void printSparqlResult(Sparql sparql)
	{
		   
	 		try {
	 			JAXBContext jax = JAXBContext.newInstance(Sparql.class) ;
	 			Marshaller m = jax.createMarshaller();
	 			StringWriter sr = new StringWriter();
	 			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
	 			m.marshal(sparql,sr);
	 			logger.info(sr.toString());

	 			
	 		} catch (JAXBException e) {
	 			// TODO Auto-generated catch block
	 			e.printStackTrace();
	 		}         
	}
	
}
