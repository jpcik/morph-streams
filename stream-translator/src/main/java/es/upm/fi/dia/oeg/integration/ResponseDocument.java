package es.upm.fi.dia.oeg.integration;

import org.w3.sparql.results.Sparql;

import com.hp.hpl.jena.rdf.model.Model;

public class ResponseDocument 
{
	private Sparql resultSet;

	private Model rdfResultSet;
	
	public void setResultSet(Sparql resultSet) {
		this.resultSet = resultSet;
	}

	public Sparql getResultSet() {
		return resultSet;
	}

	public void setRdfResultSet(Model rdfResultSet)
	{
		this.rdfResultSet = rdfResultSet;
	}

	public Model getRdfResultSet()
	{
		return rdfResultSet;
	}
}
