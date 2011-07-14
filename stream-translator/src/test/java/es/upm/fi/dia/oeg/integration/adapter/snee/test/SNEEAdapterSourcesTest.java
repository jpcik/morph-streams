package es.upm.fi.dia.oeg.integration.adapter.snee.test;

import java.net.MalformedURLException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.data.generator.ConstantRatePushStreamGenerator;
import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.DataSourceException;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.metadata.SourceType;

public class SNEEAdapterSourcesTest
{
	private static Logger logger = Logger.getLogger(SNEEAdapterSourcesTest.class.getName());
	private static ConstantRatePushStreamGenerator generator;
	static SNEEAdapter adap = new SNEEAdapter();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		PropertyConfigurator.configure(SNEEAdapterSourcesTest.class.getClassLoader().getResource("config/log4j.properties"));
		
		Properties props = ParameterUtils.load(SNEEAdapterSourcesTest.class.getClassLoader().getResourceAsStream("snee/generator/snee.properties"));
		SNEEProperties.initialise(props );
		adap.init("snee/empty/snee.properties");

		generator = new ConstantRatePushStreamGenerator();
		generator.startTransmission();

	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		generator.stopTransmission();
	}

	@Test(expected=MalformedURLException.class)
	public void testAddBadSource() throws MalformedURLException, DataSourceException
	{
		adap.addPullSource("dsfsdfsfsfs",SourceType.SERVICE);
	}
	
	@Test(expected=DataSourceException.class)
	public void testAddBad2Source() throws MalformedURLException, DataSourceException
	{
		adap.addPullSource("http://sdfsdfsdf",SourceType.SERVICE);
	}
	
	@Test@Ignore
	public void testAddSource() throws MalformedURLException, DataSourceException
	{
		adap.addPullSource("http://webgis1.geodata.soton.ac.uk:8080/CCO/services/PullStream?wsdl",SourceType.SERVICE);
		
	}


}
