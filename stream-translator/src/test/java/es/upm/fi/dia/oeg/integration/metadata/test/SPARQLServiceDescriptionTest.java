package es.upm.fi.dia.oeg.integration.metadata.test;

import static org.junit.Assert.*;

import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

import jena.sparql;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;

import es.upm.fi.dia.oeg.integration.metadata.SPARQLServiceMetadata;


public class SPARQLServiceDescriptionTest
{

	private static Logger logger = Logger.getLogger(SPARQLServiceDescriptionTest.class.getName());


	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		
	}

	@Test
	public void testCreateRDF() throws URISyntaxException
	{
		SPARQLServiceMetadata sparlqDesc = new SPARQLServiceMetadata(new URI("mappings/cco.r2r"));
		String d = sparlqDesc.getDocument();
		logger.info(d);
		//assertNotNull(m);
	}

}
