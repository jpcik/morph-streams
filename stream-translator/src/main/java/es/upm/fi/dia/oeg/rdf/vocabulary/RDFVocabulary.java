package es.upm.fi.dia.oeg.rdf.vocabulary;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class RDFVocabulary
{
	protected static String uri = "";

	public static String getUri()
	{
		return uri;
	}
		
	protected static final Resource resource(String name)
	{ 
		return ResourceFactory.createResource(uri + name); 
	}
		
	protected static final Property property( String local )
	{ 
		return ResourceFactory.createProperty( uri, local ); 
	}

}
