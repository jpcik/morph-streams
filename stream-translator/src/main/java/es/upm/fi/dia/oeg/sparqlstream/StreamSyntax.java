package es.upm.fi.dia.oeg.sparqlstream;

import com.hp.hpl.jena.query.Syntax;

public class StreamSyntax extends Syntax 
{
	   public static final Syntax syntaxSPARQLstr
       = new StreamSyntax("http://semsorgrid4env.eu/query/SPARQLstr") ;
	   
	protected StreamSyntax(String s) 
	{
		super(s);
	}

	 static {
	        querySyntaxNames.put("sparqlstr",      StreamSyntax.syntaxSPARQLstr) ;}
}
