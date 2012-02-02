package es.upm.fi.oeg.rdf.sparql;

import java.util.Collection;

import org.w3.sparql.results.Head;
import org.w3.sparql.results.Results;
import org.w3.sparql.results.Sparql;
import org.w3.sparql.results.Variable;


public class SparqlResults 
{
	public static Sparql newSparql(Collection<String> varNames)
	{
		Sparql sparqlResult = new Sparql();
		
		Head head = new Head();
		Results res = new Results();
		for (String varName:varNames)
		{
			head.getVariable().add(newVariable(varName) );
		}
		sparqlResult.setHead(head);
		sparqlResult.setResults(res);
		return sparqlResult;
	}
	
	static Variable newVariable(String varName)
	{
		Variable var = new Variable();
		var.setName(varName);
		return var;
	}
}
