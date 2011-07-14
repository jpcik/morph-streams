package es.upm.fi.dia.oeg.integration.adapter.ssg4env.test;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.rowset.WebRowSet;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.adapter.snee.SNEEAdapter;
import es.upm.fi.dia.oeg.integration.adapter.snee.test.SNEEAdapterTest;
import es.upm.fi.dia.oeg.integration.adapter.ssg4env.SSG4EnvAdapter;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;
import eu.semsorgrid4env.service.stream.pull.InvalidCountFault;
import eu.semsorgrid4env.service.stream.pull.InvalidPositionFault;
import eu.semsorgrid4env.service.stream.pull.MaximumTuplesExceededFault;
import eu.semsorgrid4env.service.wsdai.DataResourceUnavailableFault;
import eu.semsorgrid4env.service.wsdai.InvalidConfigurationDocumentFault;
import eu.semsorgrid4env.service.wsdai.InvalidDatasetFormatFault;
import eu.semsorgrid4env.service.wsdai.InvalidExpressionFault;
import eu.semsorgrid4env.service.wsdai.InvalidLanguageFault;
import eu.semsorgrid4env.service.wsdai.InvalidPortTypeQNameFault;
import eu.semsorgrid4env.service.wsdai.InvalidResourceNameFault;
import eu.semsorgrid4env.service.wsdai.NotAuthorizedFault;
import eu.semsorgrid4env.service.wsdai.ServiceBusyFault;

public class SSG4EnvAdapterTest
{

	private static SSG4EnvAdapter adapter;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		Properties props = ParameterUtils.load(SSG4EnvAdapterTest.class.getClassLoader().getResourceAsStream("config/config_memoryStore.ssg4e.properties"));
		
		adapter = new SSG4EnvAdapter();
		adapter.init(props);
	}

	@Test@Ignore
	public void testCallSNEEStringInt() throws InvalidLanguageFault, InvalidConfigurationDocumentFault, InvalidExpressionFault, InvalidPortTypeQNameFault
	{
		//SSG4EnvAdapter adap = new SSG4EnvAdapter();
		//String stringQuery ="RSTREAM SELECT * FROM windsamples[FROM NOW - 1 SECONDS TO NOW - 0 SECONDS SLIDE 1 SECONDS];";
		String stringQuery ="(SELECT hs AS waveheight, DateTime AS wavets FROM envdata_Goodwin);";
		adapter.invokeQueryFactory(stringQuery, 5);
		
	}
	
	@Test@Ignore
	public void testCallSNEEPull() throws MaximumTuplesExceededFault, InvalidResourceNameFault, InvalidPositionFault, DataResourceUnavailableFault, InvalidCountFault, ServiceBusyFault, NotAuthorizedFault, InvalidDatasetFormatFault, SQLException
	{
		//adapter.pull();
	}
	
	@Test@Ignore
	public void testGetPropertyDocument()
	{
		
	}
	

}
