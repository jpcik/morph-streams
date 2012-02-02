package es.upm.fi.dia.oeg.rdf.vocabulary;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class SSN 
{
	private static final String uri = "http://purl.oclc.org/NET/ssnx/ssn#"; 

	public static Resource observationType = resource("Observation"); 
	public static Resource sensorOutputType = resource("SensorOutput");
	public static Resource observationValueType = resource("ObservationValue"); 
	public static Resource featureType = resource("FeatureOfInterest");
	public static Resource sensorType = resource("Sensor");
	public static Resource systemType = resource("System");
	public static Resource deploymentType = resource("Deployment");
	public static Resource platformType = resource("Platform");
	
	
	public static Property hasValue = property("hasValue");
	public static Property observedProperty = property("observedProperty");
	public static Property observedBy = property("observedBy");
	public static Property featureOfInterest = property("featureOfInterest");
	public static Property deployedSystem = property("deployedSystem");
	public static Property hasDeployment = property("hasDeployment");
	public static Property hasSubSystem = property("hasSubSystem");
	public static Property onPlatform = property("onPlatform");
	public static Property observes =  property("observes");
	public static Property isPropertyOf = property("isPropertyOf");
	public static Property hasProperty = property("hasProperty");
	public static Property ofFeature = property("ofFeature");
	public static Property observationResult = property("observationResult");
	public static Property observationResultTime = property("observationResultTime");
	public static Property startTime = property("startTime");
	public static Property endTime = property("endTime");
	
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
