package es.upm.fi.oeg.integration.adapter.esper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;

import com.espertech.esper.client.Configuration;

import es.upm.fi.dia.oeg.common.ParameterUtils;
import es.upm.fi.dia.oeg.integration.QueryException;
import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.oeg.integration.adapter.esper.model.Stream;

import junit.framework.TestCase;

public class EsperAdapterTest 
{
	static Properties props;

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
	public void testRegisterQuery() throws StreamAdapterException 
	{
		EsperAdapter esper = new EsperAdapter();
		Properties props = new Properties();
		Configuration configuration = new Configuration();
        configuration.addEventType("Stream", Stream.class);

		props.put("configuration", configuration);
		esper.init(props);
		//EsperStatement st = esper.registerQuery("select * from Stream");
		EsperListener listener = new EsperListener();
		//st.addListener(listener);
	}
	
	@Test
	public void testPullNewestData() throws StreamAdapterException, QueryException, SQLException
	{
		EsperAdapter esper = new EsperAdapter();
		esper.init(props);
		
		String id =  esper.invokeQueryFactory("SELECT * FROM Stream.win:keepall();", 2);
		
		esper.sendEvent(new Stream(5.4));
		esper.sendEvent(new Stream(3.4));
		esper.sendEvent(new Stream(4.4));

		List<ResultSet> data = esper.pullNewestData(id);
		while (data.get(0).next())
		{
			System.out.println("lala");
		}
		
	}

}
