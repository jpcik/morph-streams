package es.upm.fi.dia.oeg.integration.adapter.snee.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3.sparql.results.Sparql;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator;
import uk.ac.manchester.cs.snee.evaluator.types.TaggedTuple;
import uk.ac.manchester.cs.snee.evaluator.types.Tuple;
import uk.ac.manchester.cs.snee.evaluator.types.Window;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.QueryCompilerException;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEqlQuery;
import es.upm.fi.dia.oeg.integration.test.SemanticIntegratorTest;
import es.upm.fi.dia.oeg.integration.translation.QueryTranslator;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import es.upm.fi.dia.oeg.sparqlstream.SparqlUtils;

public class SNEEAdapterTest {

	private static Logger logger = Logger.getLogger(SNEEAdapterTest.class.getName());
	private static ConstantRatePushStreamGenerator generator;
	static SNEEAdapter adap = new SNEEAdapter();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		PropertyConfigurator.configure(SNEEAdapterTest.class.getClassLoader().getResource("config/log4j.properties"));
		
		Properties props = ParameterUtils.load(SNEEAdapterTest.class.getClassLoader().getResourceAsStream("snee/generator/snee.properties"));
		SNEEProperties.initialise(props );
		adap.init("snee/generator/snee.properties");

		generator = new ConstantRatePushStreamGenerator();
		generator.startTransmission();

	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		generator.stopTransmission();
	}

	
	
	@Test//@Ignore
	public void callSNEEAdapter() throws QueryException
	{
		//SNEEAdapter adap = new SNEEAdapter();
		//try {	adap.addSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");
		//} catch (MalformedURLException e) {	e.printStackTrace();}
		//String stringQuery ="RSTREAM SELECT * FROM windsamples[FROM NOW - 1 SECONDS TO NOW - 0 SECONDS SLIDE 1 SECONDS];";
		//String stringQuery ="SELECT * FROM envdata_hernebay_met [FROM NOW - 6 HOURS  TO NOW - 0 HOURS SLIDE 30 MINUTES];";
		String stringQuery ="(SELECT 'envdata_torbay' AS extentname, Hs AS waveheight, DateTime AS wavets FROM envdata_folkestone) UNION  (SELECT 'envdata_bidefordbay' AS extentname, Hs AS waveheight, DateTime AS wavets FROM envdata_milford) ;";
		//String stringQuery ="SELECT envdata_hernebay.Hs AS waveheight,envdata_hernebay.timestamp AS wavets FROM envdata_hernebay;";
		adap.invokeQueryFactory(stringQuery, 20);//,"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");
		
	}
	

	
	@Test
	public void testJoin() throws QueryException, InterruptedException, SQLException
	{
		//SNEEAdapter adap = new SNEEAdapter();
		//try {	adap.addSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");
		//} catch (MalformedURLException e) {	e.printStackTrace();}
		//String stringQuery ="RSTREAM SELECT * FROM windsamples[FROM NOW - 1 SECONDS TO NOW - 0 SECONDS SLIDE 1 SECONDS];";
		//String stringQuery ="SELECT * FROM envdata_hernebay_met [FROM NOW - 6 HOURS  TO NOW - 0 HOURS SLIDE 30 MINUTES];";
//		String stringQuery ="SELECT envdata_folkestone.Hs, envdata_milford.Hs " +
//				"FROM envdata_folkestone[FROM NOW-1 MINUTES TO NOW-0 MINUTES] envdata_folkestone, " +
//				"envdata_milford[FROM NOW-1 MINUTES TO NOW-0 MINUTES] envdata_milford " +
//				"WHERE envdata_folkestone.Hs>envdata_milford.Hs;";
		String stringQuery = "SELECT Hs AS waveheight, " +
				"Ts AS tideheight " +
				"FROM envdata_folkestone[FROM NOW-10 MINUTES TO NOW-0 MINUTES] , " +
				"envdata_boscombe_tide[FROM NOW-10 MINUTES TO NOW-0 MINUTES]  " +
				"WHERE Hs>Ts ; " ;
				//"(SELECT 'envdata_hernebay' AS extentname1, Hs AS waveheight, " +
				//"'envdata_boscombe_tide' AS extentname2, Ts AS tideheight, " +
				//"FROM envdata_hernebay,envdata_boscombe_tide WHERE tideheight < waveheight) UNION " +
				//"(SELECT 'envdata_milford' AS extentname1, Hs AS waveheight, " +
				//"'envdata_boscombe_tide' AS extentname2, Ts AS tideheight, " +
				//"FROM envdata_milford[FROM NOW-10 MINUTES TO NOW-0 MINUTES],envdata_boscombe_tide[FROM NOW-10 MINUTES TO NOW-0 MINUTES] WHERE Ts < Hs) ;";
		//String stringQuery ="SELECT envdata_hernebay.Hs AS waveheight,envdata_hernebay.timestamp AS wavets FROM envdata_hernebay;";
		String resName = adap.invokeQueryFactory(stringQuery, 20);//,"http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl");
		Thread.sleep(20000);
	
		List<ResultSet> rs = adap.pullData(resName);//, projectList,modifiers);
		SNEEAdapter.printResults(rs);

	}
	
	
	@Test
	public void testInvokeQueryFactory() throws InterruptedException, QueryException, SQLException, SNEEException
	{
		String stringQuery ="SELECT hern.Hs AS wavepeight,timestamp AS wavets FROM envdata_hernebay hern WHERE Hs<0.5;";//[FROM NOW - 1 HOUR TO NOW SLIDE 2 SECONDS] hern;";
	//String stringQuery ="SELECT envdata_hernebay.Hs AS waveheight,envdata_hernebay.timestamp AS wavets, CONCAT(Hs,Hs) AS coso FROM envdata_hernebay;";
		String resName = adap.invokeQueryFactory(stringQuery, 20);
		System.out.println("resource: "+resName);
		//boolean var = true;
		//while (var )
		{
			Thread.sleep(5000);
		}
		
		//while (true)
		{
		List<ResultSet> rs = adap.pullData(resName,3);//, projectList,modifiers);
		SNEEAdapter.printResults(rs);
		}
	}
	

}
