package es.upm.fi.oeg.integration.adapter.esper;

import java.util.Properties;

import org.junit.Test;

import com.espertech.esper.client.Configuration;

import es.upm.fi.dia.oeg.integration.StreamAdapterException;
import es.upm.fi.oeg.integration.adapter.esper.model.Stream;

import junit.framework.TestCase;

public class EsperAdapterTest extends TestCase 
{

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

}
