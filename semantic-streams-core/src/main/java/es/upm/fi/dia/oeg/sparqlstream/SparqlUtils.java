package es.upm.fi.dia.oeg.sparqlstream;

import org.w3.sparql.results.Binding;
import org.w3.sparql.results.Result;
import org.w3.sparql.results.Results;
import org.w3.sparql.results.Sparql;

public class SparqlUtils
{
	public static String print(Sparql sparqlBindings)
	{
		String buffer = "";
		Results resus = sparqlBindings.getResults();
		for (int i=0;i<resus.getResult().size();i++) 
		{
			Result r = resus.getResult().get(i);
			for (Binding b:r.getBinding())
			{
				buffer +=("Variable:" +b.getName()+								
						" - Value:"+b.getLiteral().getContent()+
						" - Type:"+b.getLiteral().getDatatype()+"\n");
			}
		}
		return buffer;
	}

}
