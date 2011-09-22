package es.upm.fi.dia.oeg.integration;

import java.util.Map;


import es.upm.fi.dia.oeg.integration.algebra.OpInterface;
import es.upm.fi.dia.oeg.r2o.plan.Attribute;

public interface SourceQuery
{
	void setOriginalQuery(String sparqlQuery);
	String getOriginalQuery();
	
	void load(OpInterface op);

	String serializeQuery();

	Map<String, Attribute> getProjectionMap();
	
	Map<String,String> getConstants();

	Map<String, String> getModifiers();

	Map<String, String> getStaticConstants();
}
