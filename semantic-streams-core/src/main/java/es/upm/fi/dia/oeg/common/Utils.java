package es.upm.fi.dia.oeg.common;

import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.log4j.Logger;
import org.w3.sparql.results.Sparql;


public class Utils 
{
	protected static Logger logger = Logger.getLogger(Utils.class.getName());

	public static void printSparqlResult(Sparql sparql)
	{		   
 		try {
 			JAXBContext jax = JAXBContext.newInstance(Sparql.class) ;
 			Marshaller m = jax.createMarshaller();
 			StringWriter sr = new StringWriter();
 			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
 			m.marshal(sparql,sr);
 			logger.info(sr.toString());
 			
 		} catch (JAXBException e) {
 			e.printStackTrace();
 		}         
	}
	
}
