package es.upm.fi.dia.oeg.rdf.vocabulary;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class Void
{
	
	private static final String uri = "http://rdfs.org/ns/void#"; 
	
	public static Resource datasetType = resource("Dataset");
	public static Property vocabulary = property("vocabulary");
	public static Property classProperty = property("class");
	public static Property classPartition = property("classPartition");
	public static Property property = property("property");
	public static Property propertyPartition = property("propertyPartition");

	
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
