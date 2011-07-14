package es.upm.fi.dia.oeg.rdf.vocabulary;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class SPARQLServiceDescription 
{

	
	private static final String uri = "http://www.w3.org/ns/sparql-service-description#";
	

	public static final Resource datasetType = resource("Dataset");
	public static final Resource serviceType = resource("Service");
	public static final Resource language = resource("Language");
	public static final Resource graphType = resource("Graph");
	public static final Property supportedLanguage = property("supportedLanguage");
	public static final Property defaultDataset = property("defaultDatasetDescription");
	public static final Property defaultGraph = property("defaultGraph");
	public static final Property namedGraph = property("namedGraph");
	public static final Property url = property("url");
	public static final Property named = property("named");
	public static final Property name = property("name");

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
